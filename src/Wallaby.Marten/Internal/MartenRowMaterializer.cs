using System.Diagnostics.CodeAnalysis;
using System.Text;
using Marten;
using Wallaby.Abstractions;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Marten.Internal;

/// <summary>
/// Materializes Marten document-table changes: rehydrates the document from <c>data</c> through the
/// store's serializer, and translates soft deletes into Delete events (the sink document is removed).
/// Decision table: unknown table → benign skip; backfill read of a soft-deleted row → skip (matching the
/// Delete-event semantics); hard delete → key-only row; update flipping <c>mt_deleted</c> on → key-only
/// row with a Delete action; everything else deserializes <c>data</c>, falling back to the old tuple for
/// an unchanged-TOAST value (present via <c>REPLICA IDENTITY FULL</c>).
/// </summary>
internal sealed class MartenRowMaterializer : IRowMaterializer
{
    private readonly Dictionary<(string Schema, string Table), MartenTablePlan> _plans;
    private readonly ISerializer _serializer;

    public MartenRowMaterializer(IEnumerable<MartenTablePlan> plans, ISerializer serializer)
    {
        _plans = plans.ToDictionary(p => (p.Table.Schema, p.Table.TableName));
        _serializer = serializer;
    }

    public bool TryMaterialize(RawChange change, [NotNullWhen(true)] out MaterializedRow? row)
    {
        row = null;
        if (!_plans.TryGetValue((change.Schema, change.TableName), out var plan))
        {
            return false; // table not part of the model — benign skip
        }

        if (change.Action == ChangeAction.Delete)
        {
            var oldValues = change.OldValues
                ?? throw new InvalidOperationException(
                    $"Delete for '{plan.Table.QualifiedName}' carried no old values; the replica identity " +
                    "provides the key columns, so this indicates a decoding problem.");
            row = KeyOnlyRow(plan, oldValues, deleted: WasDeleted(plan, oldValues));
            return true;
        }

        // Insert/update/read. A row whose mt_deleted flag is set is not a live document: a backfill read
        // skips it (the sink doc should not exist), and a live change becomes a Delete event.
        if (plan.SoftDeleted && Find(change.NewValues, plan.DeletedColumnName)?.Value is true)
        {
            if (change.Action == ChangeAction.Read)
            {
                return false;
            }
            row = KeyOnlyRow(plan, change.NewValues, deleted: true);
            return true;
        }

        var json = ReadData(plan, change);
        object entity;
        try
        {
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            entity = _serializer.FromJson(plan.DocumentType, stream);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to deserialize '{plan.Table.QualifiedName}' data into '{plan.DocumentType.FullName}' " +
                "with the store's serializer.", ex);
        }

        var record = new Dictionary<string, object?>
        {
            [plan.IdPropertyName] = ReadKeyValue(plan, change.NewValues, "id", plan.IdType),
        };
        if (plan.Conjoined)
        {
            record["TenantId"] = ReadKeyValue(plan, change.NewValues, plan.TenantColumnName, typeof(string));
        }
        if (plan.SoftDeleted)
        {
            record["Deleted"] = false;
        }

        row = new MaterializedRow(
            change.Action, entity, record, Changes: null, PrimaryKeyValues(plan, record), plan.DocumentType);
        return true;
    }

    /// <summary>A row for a (hard or soft) delete: key values only, routed as a Delete.</summary>
    private static MaterializedRow KeyOnlyRow(MartenTablePlan plan, IReadOnlyList<RawColumn> values, bool deleted)
    {
        var record = new Dictionary<string, object?>
        {
            [plan.IdPropertyName] = ReadKeyValue(plan, values, "id", plan.IdType),
        };
        if (plan.Conjoined)
        {
            record["TenantId"] = ReadKeyValue(plan, values, plan.TenantColumnName, typeof(string));
        }
        if (plan.SoftDeleted)
        {
            record["Deleted"] = deleted;
        }

        return new MaterializedRow(
            ChangeAction.Delete, Entity: null, record, Changes: null, PrimaryKeyValues(plan, record), plan.DocumentType);
    }

    /// <summary>Primary key values in the captured key's column order, from the already-coerced record.</summary>
    private static object[] PrimaryKeyValues(MartenTablePlan plan, Dictionary<string, object?> record)
    {
        var key = new object[plan.Table.PrimaryKey.Count];
        for (var i = 0; i < key.Length; i++)
        {
            key[i] = record[plan.Table.PrimaryKey[i].PropertyName]
                ?? throw new InvalidOperationException(
                    $"Missing primary key value '{plan.Table.PrimaryKey[i].ColumnName}' for '{plan.Table.QualifiedName}'.");
        }
        return key;
    }

    private static object? ReadKeyValue(MartenTablePlan plan, IReadOnlyList<RawColumn> values, string column, Type clrType)
    {
        var raw = Find(values, column)
            ?? throw new InvalidOperationException(
                $"Column '{column}' was not present in the change for '{plan.Table.QualifiedName}'.");
        return ValueCoercion.ToClr(raw.Value, clrType);
    }

    /// <summary>The document JSON: the new tuple's <c>data</c>, or the old tuple's when unchanged TOAST kept it off the wire.</summary>
    private string ReadData(MartenTablePlan plan, RawChange change)
    {
        var column = Find(change.NewValues, "data");
        if (column is { IsUnchangedToast: false })
        {
            return AsJson(plan, column.Value);
        }

        var old = change.OldValues is { } oldValues ? Find(oldValues, "data") : null;
        if (old is { IsUnchangedToast: false, Value: not null })
        {
            return AsJson(plan, old.Value);
        }

        throw new InvalidOperationException(
            $"The document body for '{plan.Table.QualifiedName}' was not carried in the change (an unchanged " +
            $"TOASTed value with no old tuple). Run: ALTER TABLE {plan.Table.QualifiedName} REPLICA IDENTITY FULL; " +
            "— self-config warns with this DDL at startup (or fails when RequireFullReplicaIdentity is set).");
    }

    private static string AsJson(MartenTablePlan plan, object? value)
        => value switch
        {
            string json => json,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            _ => throw new InvalidOperationException(
                $"The 'data' column for '{plan.Table.QualifiedName}' produced " +
                $"'{value?.GetType().Name ?? "null"}' instead of JSON text."),
        };

    private static bool WasDeleted(MartenTablePlan plan, IReadOnlyList<RawColumn> oldValues)
        => plan.SoftDeleted && Find(oldValues, plan.DeletedColumnName)?.Value is true;

    private static RawColumn? Find(IReadOnlyList<RawColumn> values, string columnName)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (values[i].ColumnName == columnName)
            {
                return values[i];
            }
        }
        return null;
    }
}
