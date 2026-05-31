using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Internal.Materialization;

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

        var entity = plan.Factory();
        var record = new Dictionary<string, object?>(plan.Columns.Count);

        // Iterate the source columns once and look up the per-table plan, avoiding a per-call
        // Dictionary<string, RawColumn> allocation.
        for (var i = 0; i < source.Count; i++)
        {
            var column = source[i];
            if (column.IsUnchangedToast) continue;
            if (!plan.ColumnsByName.TryGetValue(column.ColumnName, out var columnPlan)) continue;

            var modelValue = ValueCoercion.ToModelValue(column.Value, columnPlan.ClrType, columnPlan.Converter);
            if (modelValue is null && !columnPlan.AcceptsNull)
            {
                // pgoutput emits non-identity columns as nulls on DELETE/REPLICA IDENTITY DEFAULT.
                // The freshly-constructed entity already has default(T) for the property; skip the set.
                record[columnPlan.PropertyName] = null;
                continue;
            }
            columnPlan.Setter?.SetClrValue(entity, modelValue);
            record[columnPlan.PropertyName] = modelValue;
        }

        var pkPlans = plan.PrimaryKey;
        var primaryKey = new object[pkPlans.Count];
        for (var i = 0; i < pkPlans.Count; i++)
        {
            primaryKey[i] = record[pkPlans[i].PropertyName]
                ?? throw new InvalidOperationException("Missing primary key value");
        }

        IReadOnlyDictionary<string, object?>? changes = null;
        if (change.Action == ChangeAction.Update && change.OldValues is { Count: > 0 } oldValues)
        {
            changes = BuildChanges(plan, oldValues, record);
        }

        row = new MaterializedRow(entity, record, changes, primaryKey, plan.ClrType);
        return true;
    }

    private static Dictionary<string, object?> BuildChanges(
        EntityPlan plan, IReadOnlyList<RawColumn> oldValues, IReadOnlyDictionary<string, object?> newRecord)
    {
        var changes = new Dictionary<string, object?>();

        for (var i = 0; i < oldValues.Count; i++)
        {
            var column = oldValues[i];
            if (column.IsUnchangedToast) continue;
            if (!plan.ColumnsByName.TryGetValue(column.ColumnName, out var columnPlan)) continue;

            var oldValue = ValueCoercion.ToModelValue(column.Value, columnPlan.ClrType, columnPlan.Converter);
            var hasNew = newRecord.TryGetValue(columnPlan.PropertyName, out var newValue);
            if (!hasNew || !Equals(oldValue, newValue))
            {
                changes[columnPlan.PropertyName] = oldValue;
            }
        }

        return changes;
    }

    private static EntityPlan BuildPlan(IEntityType entityType, string table)
    {
        var storeObject = StoreObjectIdentifier.Table(table, entityType.GetSchema());

        var columns = new List<ColumnPlan>();
        var byProperty = new Dictionary<string, ColumnPlan>();
        var byColumn = new Dictionary<string, ColumnPlan>();
        foreach (var property in entityType.GetProperties())
        {
            var columnName = property.GetColumnName(storeObject);
            if (columnName is null) continue;

            // GetSetter() returns EF Core's compiled IClrPropertySetter, which already handles
            // backing fields, shadow properties (no-op), and the PropertyBag indexer used by
            // shared-type entities (e.g. skip-navigation join tables). It's marked internal but
            // is the standard escape hatch used by EF providers/extensions.
            var plan = new ColumnPlan
            {
                ColumnName = columnName,
                PropertyName = property.Name,
                ClrType = property.ClrType,
                AcceptsNull = !property.ClrType.IsValueType || Nullable.GetUnderlyingType(property.ClrType) is not null,
                Converter = property.GetValueConverter(),
#pragma warning disable EF1001
                Setter = property.IsShadowProperty() ? null : ((IRuntimePropertyBase)property).GetSetter(),
#pragma warning restore EF1001
            };
            columns.Add(plan);
            byProperty[property.Name] = plan;
            byColumn[columnName] = plan;
        }

        var primaryKey = entityType.FindPrimaryKey()!.Properties
            .Select(p => byProperty[p.Name])
            .ToList();

        var clrType = entityType.ClrType;
        return new EntityPlan
        {
            ClrType = clrType,
            Factory = BuildFactory(clrType),
            Columns = columns,
            ColumnsByName = byColumn,
            PrimaryKey = primaryKey,
        };
    }

    private static Func<object> BuildFactory(Type clrType)
    {
        // For types without an accessible parameterless ctor (rare in EF models), fall back to Activator.
        var ctor = clrType.GetConstructor(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null, types: Type.EmptyTypes, modifiers: null);
        if (ctor is null)
        {
            return () => Activator.CreateInstance(clrType)!;
        }
        var newExpr = Expression.New(ctor);
        return Expression.Lambda<Func<object>>(Expression.Convert(newExpr, typeof(object))).Compile();
    }

    private sealed class EntityPlan
    {
        public required Type ClrType { get; init; }
        public required Func<object> Factory { get; init; }
        public required IReadOnlyList<ColumnPlan> Columns { get; init; }
        public required IReadOnlyDictionary<string, ColumnPlan> ColumnsByName { get; init; }
        public required IReadOnlyList<ColumnPlan> PrimaryKey { get; init; }
    }

    private sealed class ColumnPlan
    {
        public required string ColumnName { get; init; }
        public required string PropertyName { get; init; }
        public required Type ClrType { get; init; }
        public required bool AcceptsNull { get; init; }
        public ValueConverter? Converter { get; init; }
        public IClrPropertySetter? Setter { get; init; }
    }
}
