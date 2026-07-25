using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Marten;
using Wallaby.Abstractions;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Providers.Marten.Internal;

/// <summary>
/// Materializes Marten document-table changes: rehydrates the document from <c>data</c> through the
/// store's serializer, and translates soft deletes into Delete events (the sink document is removed).
/// Decision table: unknown table → benign skip; backfill read of a soft-deleted row → skip (matching the
/// Delete-event semantics); hard delete → Delete row, rehydrating the entity from the old tuple's
/// <c>data</c> when the wire carried it (<c>REPLICA IDENTITY FULL</c>); update flipping <c>mt_deleted</c>
/// on → Delete row rehydrated likewise; everything else deserializes <c>data</c>, falling back to the old
/// tuple for an unchanged-TOAST value.
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
            row = DeleteRow(plan, oldValues, deleted: WasDeleted(plan, oldValues), EntityOrNull(plan, change));
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
            row = DeleteRow(plan, change.NewValues, deleted: true, EntityOrNull(plan, change));
            return true;
        }

        var data = ReadData(plan, change);
        var entity = DeserializeDocument(plan, data);

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

    /// <summary>
    /// A row for a (hard or soft) delete, routed as a Delete. <paramref name="entity"/> carries the
    /// document when the wire had its body, so KeyedBy/ScopedBy can compute delete-time identity.
    /// </summary>
    private static MaterializedRow DeleteRow(
        MartenTablePlan plan, IReadOnlyList<RawColumn> values, bool deleted, object? entity)
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
            ChangeAction.Delete, entity, record, Changes: null, PrimaryKeyValues(plan, record), plan.DocumentType);
    }

    /// <summary>
    /// The deleted document, when its body is on the wire. Requires <c>REPLICA IDENTITY FULL</c> (enforced
    /// at startup for KeyedBy / entity-scoped mappings); without it the row stays entity-less.
    /// </summary>
    private object? EntityOrNull(MartenTablePlan plan, RawChange change)
        => TryReadData(plan, change) is { } data ? DeserializeDocument(plan, data) : null;

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

    /// <summary>
    /// The document JSON — a <c>string</c> or UTF-8 <c>byte[]</c>, kept in its wire form so
    /// <see cref="Deserialize"/> can avoid transcoding: the new tuple's <c>data</c>, or the old tuple's
    /// when unchanged TOAST kept it off the wire. Null when neither tuple carries the body.
    /// </summary>
    private static object? TryReadData(MartenTablePlan plan, RawChange change)
    {
        var column = Find(change.NewValues, "data");
        if (column is { IsUnchangedToast: false })
        {
            return AsJson(plan, column.Value);
        }

        var old = change.OldValues is { } oldValues ? Find(oldValues, "data") : null;
        return old is { IsUnchangedToast: false, Value: not null } ? AsJson(plan, old.Value) : null;
    }

    private static object ReadData(MartenTablePlan plan, RawChange change)
        => TryReadData(plan, change)
            ?? throw new InvalidOperationException(
                $"The document body for '{plan.Table.QualifiedName}' was not carried in the change (an unchanged " +
                $"TOASTed value with no old tuple). Run: ALTER TABLE {plan.Table.QualifiedName} REPLICA IDENTITY FULL; " +
                "— self-config warns with this DDL at startup (or fails when RequireFullReplicaIdentity is set). " +
                "See https://wallabycdc.net/providers/marten/#managed-replica-identity");

    private object DeserializeDocument(MartenTablePlan plan, object data)
    {
        try
        {
            return Deserialize(plan.DocumentType, data);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to deserialize '{plan.Table.QualifiedName}' data into '{plan.DocumentType.FullName}' " +
                "with the store's serializer.", ex);
        }
    }

    private static object AsJson(MartenTablePlan plan, object? value)
        => value switch
        {
            string or byte[] => value,
            _ => throw new InvalidOperationException(
                $"The 'data' column for '{plan.Table.QualifiedName}' produced " +
                $"'{value?.GetType().Name ?? "null"}' instead of JSON text."),
        };

    private object Deserialize(Type documentType, object data)
    {
        if (data is byte[] bytes)
        {
            using var stream = new MemoryStream(bytes, 0, bytes.Length, writable: false);
            return _serializer.FromJson(documentType, stream);
        }

        // The built-in producers deliver the body as UTF-8 bytes (handled above); a string body — e.g.
        // from a custom producer — is encoded into a pooled buffer, since a fresh byte[] per row would
        // put large documents straight on the LOH.
        var json = (string)data;
        var buffer = ArrayPool<byte>.Shared.Rent(Encoding.UTF8.GetByteCount(json));
        try
        {
            var length = Encoding.UTF8.GetBytes(json, buffer);
            using var stream = new MemoryStream(buffer, 0, length, writable: false);
            return _serializer.FromJson(documentType, stream);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

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
