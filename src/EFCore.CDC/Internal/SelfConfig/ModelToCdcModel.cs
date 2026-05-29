using EFCore.CDC.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.CDC.Internal.SelfConfig;

/// <summary>
/// Resolves a <see cref="CdcModel"/> from an EF Core <see cref="IModel"/> and a <see cref="CaptureSpec"/>.
/// Declared entities fail fast on problems (no PK, owned, view); the "all mapped" mode silently skips
/// entities that can't be captured (owned, keyless, or not table-backed).
/// </summary>
internal static class ModelToCdcModel
{
    public static CdcModel Build(IModel model, CaptureSpec spec)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(spec);

        return spec.CaptureAllMapped
            ? BuildFromAllMapped(model, spec)
            : BuildFromDeclared(model, spec);
    }

    private static CdcModel BuildFromDeclared(IModel model, CaptureSpec spec)
    {
        if (spec.DeclaredEntities.Count == 0)
        {
            throw new CdcConfigurationException(
                "No tables were declared for capture. Declare each table with Capture<T>() or Map<T>(), " +
                "or opt into CaptureAllMappedTables().");
        }

        var tables = new List<CapturedTable>(spec.DeclaredEntities.Count);
        foreach (var clrType in spec.DeclaredEntities.Distinct())
        {
            var entityType = model.FindEntityType(clrType)
                ?? throw new CdcConfigurationException(
                    $"Entity '{clrType.FullName}' was declared for capture but is not part of the DbContext model.");

            if (entityType.IsOwned())
            {
                throw new CdcConfigurationException(
                    $"Entity '{clrType.FullName}' is an owned type and cannot be captured directly; capture its owner instead.");
            }

            if (entityType.GetTableName() is null)
            {
                throw new CdcConfigurationException(
                    $"Entity '{clrType.FullName}' is not mapped to a table (it may be a view or keyless type) and cannot be captured.");
            }

            if (entityType.FindPrimaryKey() is null)
            {
                throw new CdcConfigurationException(
                    $"Entity '{clrType.FullName}' has no primary key. pgoutput logical replication requires a primary key to capture changes.");
            }

            tables.Add(BuildTable(entityType, spec.RequiresFullReplicaIdentity.Contains(clrType)));
        }

        return new CdcModel(tables);
    }

    private static CdcModel BuildFromAllMapped(IModel model, CaptureSpec spec)
    {
        var tables = new List<CapturedTable>();
        var seen = new HashSet<(string, string)>();

        foreach (var entityType in model.GetEntityTypes())
        {
            if (entityType.IsOwned()) continue;
            if (entityType.GetTableName() is null) continue;     // view / keyless / unmapped
            if (entityType.FindPrimaryKey() is null) continue;   // cannot capture without a PK

            var schema = entityType.GetSchema() ?? "public";
            var tableName = entityType.GetTableName()!;
            if (!seen.Add((schema, tableName))) continue;        // de-dup shared tables (e.g. TPH)

            var requiresFull = entityType.ClrType is { } clr && spec.RequiresFullReplicaIdentity.Contains(clr);
            tables.Add(BuildTable(entityType, requiresFull));
        }

        return new CdcModel(tables);
    }

    private static CapturedTable BuildTable(IEntityType entityType, bool requiresFullReplicaIdentity)
    {
        var schema = entityType.GetSchema();
        var tableName = entityType.GetTableName()!;
        var storeObject = StoreObjectIdentifier.Table(tableName, schema);

        var primaryKey = entityType.FindPrimaryKey()!;
        var pkPropertyNames = primaryKey.Properties.Select(p => p.Name).ToHashSet();

        var columnsByProperty = new Dictionary<string, CapturedColumn>();
        var columns = new List<CapturedColumn>();
        foreach (var property in entityType.GetProperties())
        {
            var columnName = property.GetColumnName(storeObject);
            if (columnName is null) continue; // property not mapped to this table

            var column = new CapturedColumn
            {
                PropertyName = property.Name,
                ColumnName = columnName,
                ClrType = property.ClrType,
                IsPrimaryKey = pkPropertyNames.Contains(property.Name),
            };
            columns.Add(column);
            columnsByProperty[property.Name] = column;
        }

        // Preserve primary-key ordinal order.
        var pkColumns = primaryKey.Properties
            .Select(p => columnsByProperty[p.Name])
            .ToList();

        return new CapturedTable
        {
            EntityClrType = entityType.ClrType,
            Schema = schema ?? "public",
            TableName = tableName,
            Columns = columns,
            PrimaryKey = pkColumns,
            RequiresFullReplicaIdentity = requiresFullReplicaIdentity,
        };
    }
}
