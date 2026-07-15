using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Providers.EntityFrameworkCore.Internal;

/// <summary>
/// Resolves a <see cref="WallabyModel"/> from an EF Core <see cref="IModel"/> and a <see cref="CaptureSpec"/>.
/// Declared entities fail fast on problems (no PK, owned, view). Per-mapping
/// <c>DependsOn(...)</c> navigation expressions are resolved through <see cref="DependencyAnalyzer"/>
/// — pulling additional dependent tables into the capture set and emitting the fan-out
/// <see cref="DependentBinding"/>s the live pipeline uses.
/// </summary>
internal static class EfCoreCaptureModelBuilder
{
    public static WallabyModel Build(
        IModel model, CaptureSpec spec,
        IReadOnlyDictionary<Type, IReadOnlySet<string>>? consumedProperties = null)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(spec);
        consumedProperties ??= ColumnConsumptionResolver.Resolve(model, spec);

        var primaries = BuildPrimariesFromDeclared(model, spec, consumedProperties);

        var (allTables, bindings) = AttachDependents(model, primaries, spec, consumedProperties);
        return new WallabyModel(allTables, bindings);
    }

    private static List<(IEntityType EntityType, CapturedTable Table)> BuildPrimariesFromDeclared(
        IModel model, CaptureSpec spec, IReadOnlyDictionary<Type, IReadOnlySet<string>> consumedProperties)
    {
        // An empty spec builds an empty model: with several providers registered, one of them may simply
        // have no mapped entities. "No mappings at all" is rejected once, at WallabyBuilder.Build().
        var primaries = new List<(IEntityType, CapturedTable)>(spec.DeclaredEntities.Count);
        foreach (var clrType in spec.DeclaredEntities.Distinct())
        {
            var entityType = model.FindEntityType(clrType)
                ?? throw new WallabyConfigurationException(
                    $"Entity '{clrType.FullName}' was declared for capture but is not part of the DbContext model.");

            if (entityType.IsOwned())
            {
                throw new WallabyConfigurationException(
                    $"Entity '{clrType.FullName}' is an owned type and cannot be captured directly; capture its owner instead.");
            }

            if (entityType.GetTableName() is null)
            {
                throw new WallabyConfigurationException(
                    $"Entity '{clrType.FullName}' is not mapped to a table (it may be a view or keyless type) and cannot be captured.");
            }

            if (entityType.FindPrimaryKey() is null)
            {
                throw new WallabyConfigurationException(
                    $"Entity '{clrType.FullName}' has no primary key. pgoutput logical replication requires a primary key to capture changes.");
            }

            // A discriminator property exists exactly for TPH hierarchy members; TPT/TPC don't share tables.
            if (entityType.FindDiscriminatorProperty() is not null
                && (entityType.BaseType is not null || entityType.GetDirectlyDerivedTypes().Any()))
            {
                throw new WallabyConfigurationException(
                    $"Entity '{clrType.FullName}' is part of a TPH hierarchy, which Wallaby cannot capture: " +
                    "rows would materialize as one arbitrary hierarchy type and lose subclass data. " +
                    "Use TPT or TPC mapping for captured hierarchies.");
            }

            primaries.Add((entityType, BuildTable(
                entityType, spec.RequiresFullReplicaIdentity.Contains(clrType),
                consumedProperties.GetValueOrDefault(clrType))));
        }

        return primaries;
    }

    private static (IReadOnlyList<CapturedTable> All, IReadOnlyList<DependentBinding> Bindings) AttachDependents(
        IModel model, List<(IEntityType EntityType, CapturedTable Table)> primaries, CaptureSpec spec,
        IReadOnlyDictionary<Type, IReadOnlySet<string>> consumedProperties)
    {
        // Union of primary + dependent tables, de-duplicated by (schema, table). A table that is both
        // primary-mapped and the target of a DependsOn appears once — the primary capture is canonical.
        var byQualifiedName = primaries.ToDictionary(
            p => (p.Table.Schema, p.Table.TableName),
            p => p.Table);
        var bindings = new List<DependentBinding>();

        foreach (var (entityType, primaryTable) in primaries)
        {
            if (!spec.DeclaredDependencies.TryGetValue(entityType.ClrType, out var expressions) || expressions.Count == 0)
            {
                continue;
            }

            // The same navigation may be declared by several of the entity's mappings (one per sink);
            // it fans out once. Dedupe by the resolved shape: dependent table + lookup columns.
            var seen = new HashSet<(string Schema, string Table, string Lookup)>();
            foreach (var expr in expressions)
            {
                var resolution = DependencyAnalyzer.Analyze(entityType, expr);
                var depTable = GetOrAddDependentTable(byQualifiedName, resolution.DependentEntityType, consumedProperties);
                var lookup = string.Join(",", resolution.Lookup.Select(l => $"{l.DependentColumn}>{l.PrimaryColumn}"));
                if (!seen.Add((depTable.Schema, depTable.TableName, lookup)))
                {
                    continue;
                }
                foreach (var lookupColumn in resolution.Lookup)
                {
                    EnsureLookupColumnCaptured(entityType, depTable, lookupColumn.DependentColumn);
                    EnsureLookupColumnCaptured(entityType, primaryTable, lookupColumn.PrimaryColumn);
                }
                bindings.Add(new DependentBinding
                {
                    PrimaryTable = primaryTable,
                    DependentTable = depTable,
                    Lookup = resolution.Lookup,
                });
            }
        }

        return (byQualifiedName.Values.ToList(), bindings);
    }

    // A DependsOn fan-out reads its lookup keys from the captured columns. The consumption resolver pins
    // lookup properties into every effective set, so this is a safety net for shapes it cannot see.
    private static void EnsureLookupColumnCaptured(IEntityType primary, CapturedTable table, string columnName)
    {
        if (table.Columns.All(c => c.ColumnName != columnName))
        {
            throw new WallabyConfigurationException(
                $"DependsOn(...) on '{primary.ClrType.Name}' resolves through column '{columnName}' of " +
                $"'{table.QualifiedName}', which its column selections do not capture. A dependency-lookup " +
                "column cannot be excluded.");
        }
    }

    private static CapturedTable GetOrAddDependentTable(
        Dictionary<(string Schema, string Table), CapturedTable> byQualifiedName, IEntityType dependentEntityType,
        IReadOnlyDictionary<Type, IReadOnlySet<string>> consumedProperties)
    {
        var schema = dependentEntityType.GetSchema() ?? "public";
        var tableName = dependentEntityType.GetTableName()
            ?? throw new WallabyConfigurationException(
                $"Dependency target '{dependentEntityType.ClrType.FullName}' has no table — it must be a table-backed entity.");

        if (byQualifiedName.TryGetValue((schema, tableName), out var existing))
        {
            return existing;
        }

        var built = BuildTable(
            dependentEntityType, requiresFullReplicaIdentity: false,
            consumedProperties.GetValueOrDefault(dependentEntityType.ClrType));
        byQualifiedName[(schema, tableName)] = built;
        return built;
    }

    private static CapturedTable BuildTable(
        IEntityType entityType, bool requiresFullReplicaIdentity, IReadOnlySet<string>? consumedProperties)
    {
        var schema = entityType.GetSchema();
        var tableName = entityType.GetTableName()!;
        var storeObject = StoreObjectIdentifier.Table(tableName, schema);

        var primaryKey = entityType.FindPrimaryKey()
            ?? throw new WallabyConfigurationException(
                $"Entity '{entityType.ClrType.FullName}' has no primary key. pgoutput logical replication requires a primary key.");
        var pkPropertyNames = primaryKey.Properties.Select(p => p.Name).ToHashSet();

        var columnsByProperty = new Dictionary<string, CapturedColumn>();
        var columns = new List<CapturedColumn>();
        foreach (var property in entityType.GetProperties())
        {
            var columnName = property.GetColumnName(storeObject);
            if (columnName is null) continue; // property not mapped to this table
            // Unselected columns are dropped from the capture set entirely (publication column list,
            // materialization, backfill reads).
            if (consumedProperties is not null && !consumedProperties.Contains(property.Name)) continue;

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
