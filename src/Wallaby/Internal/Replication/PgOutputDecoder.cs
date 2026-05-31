using Npgsql.Replication.PgOutput;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Reads pgoutput row tuples into <see cref="RawColumn"/> arrays. Crucially, this fully reads and copies
/// every column value <em>inside</em> the streaming loop iteration, because Npgsql recycles the
/// underlying message and tuple objects after each iteration.
/// </summary>
internal static class PgOutputDecoder
{
    /// <summary>Read all columns of a replication tuple, copying values out.</summary>
    public static async Task<RawColumn[]> ReadTupleAsync(ReplicationTuple tuple, CancellationToken ct)
    {
        var columns = new RawColumn[tuple.NumColumns];
        var i = 0;
        await foreach (var value in tuple.WithCancellation(ct))
        {
            columns[i++] = await ReadValueAsync(value, ct);
        }
        return columns;
    }

    private static async ValueTask<RawColumn> ReadValueAsync(ReplicationValue value, CancellationToken ct)
    {
        var columnName = value.GetFieldName();

        if (value.IsUnchangedToastedValue)
        {
            return new RawColumn { ColumnName = columnName, Value = null, IsUnchangedToast = true };
        }

        if (value.IsDBNull)
        {
            // Consume the (empty) value to keep the tuple stream positioned correctly.
            _ = await value.Get(ct);
            return new RawColumn { ColumnName = columnName, Value = null };
        }

        var decoded = await value.Get(ct);
        return new RawColumn { ColumnName = columnName, Value = decoded };
    }
}
