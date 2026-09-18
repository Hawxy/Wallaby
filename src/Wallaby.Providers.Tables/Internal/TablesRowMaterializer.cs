using System.Diagnostics.CodeAnalysis;
using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>
/// Turns decoded <see cref="RawChange"/>s into POCOs: coerces each captured column with
/// <see cref="ValueCoercion"/>, constructs the entity through the registration's constructor plan, and
/// assigns the remaining settable members. A column absent from the change (a narrowed selection, or a
/// delete under <c>REPLICA IDENTITY DEFAULT</c>) leaves its member at the default.
/// </summary>
internal sealed class TablesRowMaterializer : IRowMaterializer
{
    private readonly Dictionary<(string Schema, string Table), TablePlan> _plans;

    public TablesRowMaterializer(IEnumerable<TablePlan> plans)
    {
        _plans = plans.ToDictionary(p => (p.Table.Schema, p.Table.TableName));
    }

    public bool TryMaterialize(RawChange change, [NotNullWhen(true)] out MaterializedRow? row)
    {
        if (!_plans.TryGetValue((change.Schema, change.TableName), out var plan))
        {
            row = null;
            return false;
        }

        var source = change.Action == ChangeAction.Delete
            ? change.OldValues ?? throw new InvalidOperationException(
                $"The delete on '{plan.Table.QualifiedName}' carried no old values; the replica identity provides " +
                "the key columns, so the table is missing a primary key or REPLICA IDENTITY is NOTHING.")
            : change.NewValues;

        var captured = plan.Captured;
        var values = new object?[captured.Count];
        var present = new bool[captured.Count];
        var record = new Dictionary<string, object?>(captured.Count);
        for (var i = 0; i < source.Count; i++)
        {
            var column = source[i];
            if (!plan.ColumnsByName.TryGetValue(column.ColumnName, out var index)) continue;

            var raw = column.IsUnchangedToast ? ResolveUnchangedToast(change, column.ColumnName) : column.Value;
            var value = ValueCoercion.ToClr(raw, captured[index].ClrType);
            values[index] = value;
            present[index] = true;
            record[captured[index].PropertyName] = value;
        }

        var entity = Construct(plan, values, present);

        var key = plan.Registration.Key;
        var primaryKey = new object[key.Count];
        for (var i = 0; i < key.Count; i++)
        {
            primaryKey[i] = record.GetValueOrDefault(key[i].PropertyName)
                ?? throw new InvalidOperationException(
                    $"Key column '{key[i].ColumnName}' of '{plan.Table.QualifiedName}' was null or absent in the change.");
        }

        IReadOnlyDictionary<string, object?>? changes = null;
        if (change.Action == ChangeAction.Update && change.OldValues is { Count: > 0 } oldValues)
        {
            changes = BuildChanges(plan, oldValues, record);
        }

        row = new MaterializedRow(change.Action, entity, record, changes, primaryKey, plan.Registration.ClrType);
        return true;
    }

    private static object Construct(TablePlan plan, object?[] values, bool[] present)
    {
        var constructor = plan.Registration.Constructor;
        object instance;
        if (constructor.ParameterMembers.Length == 0)
        {
            instance = constructor.Constructor.Invoke(null);
        }
        else
        {
            var parameterTypes = constructor.ParameterTypes;
            var args = new object?[parameterTypes.Length];
            for (var i = 0; i < args.Length; i++)
            {
                var index = plan.ConstructorArguments[i];
                var value = index >= 0 && present[index] ? values[index] : null;
                args[i] = value ?? ScalarTypes.DefaultValue(parameterTypes[i]);
            }
            instance = constructor.Constructor.Invoke(args);
        }

        foreach (var index in plan.Assignments)
        {
            if (!present[index]) continue;
            var member = plan.Captured[index];
            var value = values[index];
            if (value is null && !member.AcceptsNull) continue;
            member.Property.SetValue(instance, value);
        }
        return instance;
    }

    // An unchanged TOASTed value is omitted from the new tuple; under REPLICA IDENTITY FULL the old
    // tuple still carries it. The typed exception lets the pipeline heal by reselect, or halts as a
    // poison change when that is disabled.
    private static object ResolveUnchangedToast(RawChange change, string columnName)
    {
        if (change.OldValues?.Find(columnName) is { IsUnchangedToast: false, Value: { } value })
        {
            return value;
        }

        throw new UnavailableValueException(
            change.Schema, change.TableName, columnName,
            $"Column '{columnName}' on '{change.Schema}.{change.TableName}' was not carried in the change " +
            $"(an unchanged TOASTed value with no old tuple). Run: ALTER TABLE {change.Schema}.{change.TableName} " +
            "REPLICA IDENTITY FULL; - self-config warns with this DDL at startup (or fails when " +
            "RequireFullReplicaIdentity is set). If no transform reads the value, drop it from capture via the " +
            "mapping's column selection instead (e.g. .Map<T>().ConsumesAllExcept(e => e.Payload)). See " +
            "https://wallabycdc.net/providers/tables/#replica-identity");
    }

    private static Dictionary<string, object?> BuildChanges(
        TablePlan plan, IReadOnlyList<RawColumn> oldValues, IReadOnlyDictionary<string, object?> newRecord)
    {
        var changes = new Dictionary<string, object?>();
        for (var i = 0; i < oldValues.Count; i++)
        {
            var column = oldValues[i];
            if (column.IsUnchangedToast) continue;
            if (!plan.ColumnsByName.TryGetValue(column.ColumnName, out var index)) continue;

            var member = plan.Captured[index];
            var oldValue = ValueCoercion.ToClr(column.Value, member.ClrType);
            var hasNew = newRecord.TryGetValue(member.PropertyName, out var newValue);
            if (!hasNew || !Equals(oldValue, newValue))
            {
                changes[member.PropertyName] = oldValue;
            }
        }
        return changes;
    }
}
