using JasperFx;
using JasperFx.MultiTenancy;
using Marten;
using Marten.Schema;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Marten.Internal;

/// <summary>
/// The Marten storage provider: derives the capture plan from the store's registered document mappings
/// (via <see cref="IReadOnlyStoreOptions.AllKnownDocumentTypes"/>) and materializes rows with
/// <see cref="MartenRowMaterializer"/>, rehydrating documents from <c>data</c> through the store's
/// serializer. Only documents registered with Marten up front (e.g. <c>StoreOptions.RegisterDocumentType</c>
/// or <c>Schema.For&lt;T&gt;()</c>) are visible — Marten's lazy first-use discovery cannot inform a
/// capture model built at startup.
/// </summary>
internal sealed class MartenModelProvider(IReadOnlyStoreOptions options) : IWallabyModelProvider
{
    // Marten's fixed document-table column names (the metadata column names come from the mapping).
    private const string IdColumn = "id";
    private const string DataColumn = "data";

    public CapturePlan BuildCapturePlan(CaptureSpec spec)
    {
        if (spec.DeclaredDependencies.Count > 0)
        {
            throw new WallabyConfigurationException(
                $"DependsOn is not supported for Marten documents ('{spec.DeclaredDependencies.Keys.First().Name}' " +
                "declares one); documents are self-contained.");
        }

        var plans = SelectMappings(spec).Select(mapping => BuildTablePlan(mapping, spec)).ToList();
        return new CapturePlan
        {
            Model = new WallabyModel([.. plans.Select(p => p.Table)]),
            Materializer = new MartenRowMaterializer(plans, options.Serializer()),
        };
    }

    public QualifiedTable ResolveTable(Type entityClrType)
    {
        var mapping = FindMapping(entityClrType)
            ?? throw new WallabyConfigurationException(
                $"'{entityClrType.FullName}' is not a registered Marten document type. Register it with the " +
                "store (e.g. StoreOptions.RegisterDocumentType or Schema.For<T>()).");
        return new QualifiedTable(mapping.TableName.Schema, mapping.TableName.Name);
    }

    public bool Handles(Type entityClrType) => FindMapping(entityClrType) is not null;

    private IEnumerable<DocumentMapping> SelectMappings(CaptureSpec spec)
    {
        if (spec.CaptureAllMapped)
        {
            // Hierarchies (subclass documents share the root table) are not capturable in v1: rows would
            // deserialize into the root type and silently lose subclass data.
            return KnownMappings().Where(m => !m.IsHierarchy());
        }

        return spec.DeclaredEntities.Select(type =>
        {
            var mapping = FindMapping(type)
                ?? throw new WallabyConfigurationException(
                    $"Mapped entity '{type.FullName}' is not a registered Marten document type. Register it " +
                    "with the store (e.g. StoreOptions.RegisterDocumentType or Schema.For<T>()).");
            if (mapping.IsHierarchy())
            {
                throw new WallabyConfigurationException(
                    $"'{type.FullName}' is a Marten document hierarchy, which Wallaby cannot capture yet: " +
                    "rows would deserialize into the root type and lose subclass data.");
            }
            return mapping;
        });
    }

    private MartenTablePlan BuildTablePlan(DocumentMapping mapping, CaptureSpec spec)
    {
        var conjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;
        var softDeleted = mapping.DeleteStyle == DeleteStyle.SoftDelete;

        var id = new CapturedColumn
        {
            PropertyName = mapping.IdMember.Name,
            ColumnName = IdColumn,
            ClrType = mapping.IdType,
            IsPrimaryKey = true,
        };
        var tenant = conjoined
            ? new CapturedColumn
            {
                PropertyName = "TenantId",
                ColumnName = mapping.Metadata.TenantId.Name,
                ClrType = typeof(string),
                IsPrimaryKey = true,
            }
            : null;

        // The minimal capture set: identity, the document body, and the soft-delete flags. Marten's other
        // metadata columns (mt_version, mt_last_modified, mt_dotnet_type, duplicated fields) still arrive on
        // the wire but are not modeled — the materializer ignores unmodeled columns and backfill only
        // selects modeled ones.
        var columns = new List<CapturedColumn>();
        // Match Marten's physical primary-key column order — keyset cursors and DocumentKey identity
        // depend on it (a conjoined key must include the tenant so equal ids across tenants stay distinct).
        var primaryKey = tenant is null
            ? new List<CapturedColumn> { id }
            : mapping.PrimaryKeyTenancyOrdering == PrimaryKeyTenancyOrdering.Id_Then_TenantId
                ? [id, tenant]
                : [tenant, id];
        columns.AddRange(primaryKey);
        columns.Add(new CapturedColumn
        {
            PropertyName = "Data", ColumnName = DataColumn, ClrType = typeof(string), IsPrimaryKey = false,
        });
        if (softDeleted)
        {
            columns.Add(new CapturedColumn
            {
                PropertyName = "Deleted",
                ColumnName = mapping.Metadata.IsSoftDeleted.Name,
                ClrType = typeof(bool),
                IsPrimaryKey = false,
            });
            columns.Add(new CapturedColumn
            {
                PropertyName = "DeletedAt",
                ColumnName = mapping.Metadata.SoftDeletedAt.Name,
                ClrType = typeof(DateTimeOffset?),
                IsPrimaryKey = false,
            });
        }

        return new MartenTablePlan
        {
            Table = new CapturedTable
            {
                EntityClrType = mapping.DocumentType,
                Schema = mapping.TableName.Schema,
                TableName = mapping.TableName.Name,
                Columns = columns,
                PrimaryKey = primaryKey,
                // An undelete (UPDATE … SET mt_deleted = false) doesn't assign data, so pgoutput omits the
                // TOASTed value from the new tuple; REPLICA IDENTITY FULL puts the complete old tuple on the
                // wire and the materializer falls back to it.
                RequiresFullReplicaIdentity =
                    softDeleted || spec.RequiresFullReplicaIdentity.Contains(mapping.DocumentType),
            },
            DocumentType = mapping.DocumentType,
            IdPropertyName = mapping.IdMember.Name,
            IdType = mapping.IdType,
            TenantColumnName = tenant?.ColumnName,
            DeletedColumnName = softDeleted ? mapping.Metadata.IsSoftDeleted.Name : null,
        };
    }

    private DocumentMapping? FindMapping(Type documentType)
        => KnownMappings().FirstOrDefault(m => m.DocumentType == documentType);

    // AllKnownDocumentTypes() is Marten's public, side-effect-free model surface (probing must never
    // register new document types). Subclass mappings are excluded; their root carries the table.
    private IEnumerable<DocumentMapping> KnownMappings()
        => options.AllKnownDocumentTypes().OfType<DocumentMapping>();
}
