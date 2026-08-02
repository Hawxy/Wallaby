using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Wallaby.Abstractions;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Providers.EntityFrameworkCore.Internal;

/// <summary>
/// Turns decoded <see cref="RawChange"/>s into materialized CLR entities using per-table plans built
/// once by <see cref="EntityPlanBuilder"/> and cached. Column values buffer into slots, then the
/// plan's construction tree builds the entity: same-table owned references and complex properties
/// construct with their owner (via the EF constructor binding or a parameterless constructor), and an
/// optional member whose subtree is all null stays null. For captured entity types a member that
/// cannot be constructed fails at startup rather than on the first row.
/// </summary>
internal sealed class EntityMaterializer : IRowMaterializer
{
    private readonly Dictionary<(string Schema, string Table), EntityPlan> _plans;

    public EntityMaterializer(
        IModel model,
        IReadOnlyDictionary<string, IReadOnlySet<string>>? consumedProperties = null,
        IReadOnlyCollection<Type>? capturedTypes = null)
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

            _plans[(schema, table)] = EntityPlanBuilder.Build(
                entityType, table, consumedProperties?.GetValueOrDefault(entityType.Name),
                strict: capturedTypes?.Contains(entityType.ClrType) == true);
        }
    }

    /// <summary>Materialize a change. Returns false when the change's table is not part of the model.</summary>
    public bool TryMaterialize(RawChange change, [NotNullWhen(true)] out MaterializedRow? row)
    {
        if (!_plans.TryGetValue((change.Schema, change.TableName), out var plan))
        {
            row = null;
            return false;
        }

        var source = (change.Action == ChangeAction.Delete ? change.OldValues : change.NewValues) ?? [];
        var record = new Dictionary<string, object?>(plan.ColumnsByName.Count);
        var slots = new object?[plan.SlotCount];
        var present = new bool[plan.SlotCount];

        // Iterate the source columns once and look up the per-table plan, avoiding a per-call
        // Dictionary<string, RawColumn> allocation.
        for (var i = 0; i < source.Count; i++)
        {
            var column = source[i];
            if (!plan.ColumnsByName.TryGetValue(column.ColumnName, out var columnPlan)) continue;

            var value = column.IsUnchangedToast ? ResolveUnchangedToast(change, column.ColumnName) : column.Value;
            var modelValue = ToModelValue(value, columnPlan.ClrType, columnPlan.Converter);
            slots[columnPlan.Slot] = modelValue;
            present[columnPlan.Slot] = true;
            record[columnPlan.PropertyName] = modelValue;
        }

        var entity = Construct(plan.Root, slots, present)!; // the root is never optional

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

        row = new MaterializedRow(change.Action, entity, record, changes, primaryKey, plan.ClrType);
        return true;
    }

    private static object? Construct(NodePlan node, object?[] slots, bool[] present)
    {
        if (node.IsOptional && !HasValue(node, slots))
        {
            return null;
        }

        var instance = node.Construct(slots);
        foreach (var assignment in node.Assignments)
        {
            if (!present[assignment.Slot]) continue; // column not in the change: keep the default
            var value = slots[assignment.Slot];
            if (value is null && !assignment.AcceptsNull)
            {
                // pgoutput emits non-identity columns as nulls on DELETE/REPLICA IDENTITY DEFAULT;
                // the member keeps default(T).
                continue;
            }
            assignment.Set(instance, value);
        }
        foreach (var child in node.Children)
        {
            // A child is complete (its own children attached) before it lands in the parent, and the
            // parent receives it before being attached upward itself: safe for boxed struct members.
            if (Construct(child, slots, present) is { } value)
            {
                child.Attach!(instance, value);
            }
        }
        return instance;
    }

    private static bool HasValue(NodePlan node, object?[] slots)
    {
        foreach (var slot in node.ValueSlots)
        {
            if (slots[slot] is not null) return true;
        }
        foreach (var child in node.Children)
        {
            if (HasValue(child, slots)) return true;
        }
        return false;
    }

    // An unchanged TOASTed value is omitted from the new tuple; under REPLICA IDENTITY FULL the old
    // tuple still carries it. An unavailable value is never a silently nulled property: the typed
    // exception lets the pipeline heal by reselect, or halts as a poison change when that is disabled.
    private static object ResolveUnchangedToast(RawChange change, string columnName)
    {
        if (change.OldValues is { } oldValues)
        {
            for (var i = 0; i < oldValues.Count; i++)
            {
                var old = oldValues[i];
                if (old.ColumnName == columnName && old is { IsUnchangedToast: false, Value: not null })
                {
                    return old.Value;
                }
            }
        }

        throw new UnavailableValueException(
            change.Schema, change.TableName, columnName,
            $"Column '{columnName}' on '{change.Schema}.{change.TableName}' was not carried in the change " +
            $"(an unchanged TOASTed value with no old tuple). Run: ALTER TABLE {change.Schema}.{change.TableName} " +
            "REPLICA IDENTITY FULL; - self-config warns with this DDL at startup (or fails when " +
            "RequireFullReplicaIdentity is set). If no transform reads the value, drop it from capture via the " +
            "mapping's column selection instead (e.g. .Map<T>().ConsumesAllExcept(e => e.Payload)). See " +
            "https://wallabycdc.net/providers/entity-framework-core/#replica-identity-in-migrations");
    }

    private static object? ToModelValue(object? rawValue, Type modelClrType, ValueConverter? converter)
    {
        if (converter is null)
        {
            return ValueCoercion.ToClr(rawValue, modelClrType);
        }

        if (rawValue is null)
        {
            return null;
        }

        // The converter expects the provider representation; make sure the raw value matches it first.
        var providerValue = ValueCoercion.ToClr(rawValue, converter.ProviderClrType);
        return converter.ConvertFromProvider(providerValue);
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

            var oldValue = ToModelValue(column.Value, columnPlan.ClrType, columnPlan.Converter);
            var hasNew = newRecord.TryGetValue(columnPlan.PropertyName, out var newValue);
            if (!hasNew || !Equals(oldValue, newValue))
            {
                changes[columnPlan.PropertyName] = oldValue;
            }
        }

        return changes;
    }
}
