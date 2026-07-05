using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.EntityFrameworkCore.Internal;

/// <summary>
/// Resolves a <see cref="WallabyModel"/> from an EF Core <see cref="IModel"/> and a <see cref="CaptureSpec"/>.
/// Declared entities fail fast on problems (no PK, owned, view). Per-mapping
/// <c>DependsOn(...)</c> navigation expressions are resolved through <see cref="DependencyAnalyzer"/>
/// — pulling additional dependent tables into the capture set and emitting the fan-out
/// <see cref="DependentBinding"/>s the live pipeline uses.
/// </summary>
internal static class EfCoreCaptureModelBuilder
{
    public static WallabyModel Build(IModel model, CaptureSpec spec)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(spec);

        var primaries = BuildPrimariesFromDeclared(model, spec);

        var (allTables, bindings) = AttachDependents(model, primaries, spec);
        return new WallabyModel(allTables, bindings);
    }

    private static List<(IEntityType EntityType, CapturedTable Table)> BuildPrimariesFromDeclared(IModel model, CaptureSpec spec)
    {
        if (spec.DeclaredEntities.Count == 0)
        {
            throw new WallabyConfigurationException(
                "No tables were declared for capture. Map each table with Map<T>().");
        }

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

            primaries.Add((entityType, BuildTable(entityType, spec.RequiresFullReplicaIdentity.Contains(clrType))));
        }

        return primaries;
    }

    private static (IReadOnlyList<CapturedTable> All, IReadOnlyList<DependentBinding> Bindings) AttachDependents(
        IModel model, List<(IEntityType EntityType, CapturedTable Table)> primaries, CaptureSpec spec)
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

            foreach (var expr in expressions)
            {
                var resolution = DependencyAnalyzer.Analyze(entityType, expr);
                var depTable = GetOrAddDependentTable(byQualifiedName, resolution.DependentEntityType);
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

    private static CapturedTable GetOrAddDependentTable(
        Dictionary<(string Schema, string Table), CapturedTable> byQualifiedName, IEntityType dependentEntityType)
    {
        var schema = dependentEntityType.GetSchema() ?? "public";
        var tableName = dependentEntityType.GetTableName()
            ?? throw new WallabyConfigurationException(
                $"Dependency target '{dependentEntityType.ClrType.FullName}' has no table — it must be a table-backed entity.");

        if (byQualifiedName.TryGetValue((schema, tableName), out var existing))
        {
            return existing;
        }

        var built = BuildTable(dependentEntityType, requiresFullReplicaIdentity: false);
        byQualifiedName[(schema, tableName)] = built;
        return built;
    }

    private static CapturedTable BuildTable(IEntityType entityType, bool requiresFullReplicaIdentity)
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
