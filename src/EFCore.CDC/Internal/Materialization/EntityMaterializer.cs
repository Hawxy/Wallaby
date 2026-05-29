using EFCore.CDC.Abstractions;
using EFCore.CDC.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.CDC.Internal.Materialization;

/// <summary>
/// Turns decoded <see cref="RawChange"/>s into materialized CLR entities
/// using EF Core model metadata (column-to-property mappings and value converters). Plans are computed
/// once per table and cached.
/// </summary>
internal sealed class EntityMaterializer
{
    private readonly Dictionary<(string Schema, string Table), EntityPlan> _plans;

    public EntityMaterializer(IModel model)
    {
        _plans = new Dictionary<(string, string), EntityPlan>();
        foreach (var entityType in model.GetEntityTypes())
        {
            if (entityType.IsOwned()) continue;
            var table = entityType.GetTableName();
            if (table is null) continue;
            if (entityType.FindPrimaryKey() is null) continue;

            var schema = entityType.GetSchema() ?? "public";
            if (_plans.ContainsKey((schema, table))) continue; // de-dup shared tables (TPH)

            _plans[(schema, table)] = BuildPlan(entityType, table);
        }
    }

    /// <summary>Materialize a change. Returns false when the change's table is not part of the model.</summary>
    public bool TryMaterialize(RawChange change, out MaterializedRow row)
    {
        if (!_plans.TryGetValue((change.Schema, change.TableName), out var plan))
        {
            row = null!;
            return false;
        }

        var source = (change.Action == ChangeAction.Delete ? change.OldValues : change.NewValues) ?? [];
        var sourceByColumn = ToColumnMap(source);

        var entity = plan.Factory();
        var record = new Dictionary<string, object?>(plan.Columns.Count);
        foreach (var column in plan.Columns)
        {
            if (!sourceByColumn.TryGetValue(column.ColumnName, out var raw) || raw.IsUnchangedToast)
            {
                continue; // value not present (or unchanged-TOAST): leave default, omit from record
            }

            var modelValue = ValueCoercion.ToModelValue(raw.Value, column.ClrType, column.Converter);
            column.Setter?.Invoke(entity, modelValue);
            record[column.PropertyName] = modelValue;
        }

        var primaryKey = plan.PrimaryKey
            .Select(pk => record.TryGetValue(pk.PropertyName, out var value) ? value : null)
            .ToList();

        IReadOnlyDictionary<string, object?>? changes = null;
        if (change.Action == ChangeAction.Update && change.OldValues is { Count: > 0 })
        {
            changes = BuildChanges(plan, change.OldValues, record);
        }

        row = new MaterializedRow(entity, record, changes, primaryKey, plan.ClrType);
        return true;
    }

    private static Dictionary<string, object?> BuildChanges(
        EntityPlan plan, IReadOnlyList<RawColumn> oldValues, IReadOnlyDictionary<string, object?> newRecord)
    {
        var oldByColumn = ToColumnMap(oldValues);
        var changes = new Dictionary<string, object?>();

        foreach (var column in plan.Columns)
        {
            if (!oldByColumn.TryGetValue(column.ColumnName, out var raw) || raw.IsUnchangedToast)
            {
                continue;
            }

            var oldValue = ValueCoercion.ToModelValue(raw.Value, column.ClrType, column.Converter);
            var hasNew = newRecord.TryGetValue(column.PropertyName, out var newValue);
            if (!hasNew || !Equals(oldValue, newValue))
            {
                changes[column.PropertyName] = oldValue;
            }
        }

        return changes;
    }

    private static Dictionary<string, RawColumn> ToColumnMap(IReadOnlyList<RawColumn> columns)
    {
        var map = new Dictionary<string, RawColumn>(columns.Count);
        foreach (var column in columns)
        {
            map[column.ColumnName] = column;
        }
        return map;
    }

    private static EntityPlan BuildPlan(IEntityType entityType, string table)
    {
        var storeObject = StoreObjectIdentifier.Table(table, entityType.GetSchema());

        var columns = new List<ColumnPlan>();
        var byProperty = new Dictionary<string, ColumnPlan>();
        foreach (var property in entityType.GetProperties())
        {
            var columnName = property.GetColumnName(storeObject);
            if (columnName is null) continue;

            var plan = new ColumnPlan
            {
                ColumnName = columnName,
                PropertyName = property.Name,
                ClrType = property.ClrType,
                Converter = property.GetValueConverter(),
                Setter = BuildSetter(property),
            };
            columns.Add(plan);
            byProperty[property.Name] = plan;
        }

        var primaryKey = entityType.FindPrimaryKey()!.Properties
            .Select(p => byProperty[p.Name])
            .ToList();

        var clrType = entityType.ClrType;
        return new EntityPlan
        {
            ClrType = clrType,
            Factory = () => Activator.CreateInstance(clrType)!,
            Columns = columns,
            PrimaryKey = primaryKey,
        };
    }

    private static Action<object, object?>? BuildSetter(IProperty property)
    {
        if (property.PropertyInfo is { CanWrite: true } propertyInfo)
        {
            return (entity, value) => propertyInfo.SetValue(entity, value);
        }

        if (property.FieldInfo is { } fieldInfo)
        {
            return (entity, value) => fieldInfo.SetValue(entity, value);
        }

        return null; // shadow property: recorded but not settable on the CLR instance
    }

    private sealed class EntityPlan
    {
        public required Type ClrType { get; init; }
        public required Func<object> Factory { get; init; }
        public required IReadOnlyList<ColumnPlan> Columns { get; init; }
        public required IReadOnlyList<ColumnPlan> PrimaryKey { get; init; }
    }

    private sealed class ColumnPlan
    {
        public required string ColumnName { get; init; }
        public required string PropertyName { get; init; }
        public required Type ClrType { get; init; }
        public ValueConverter? Converter { get; init; }
        public Action<object, object?>? Setter { get; init; }
    }
}
