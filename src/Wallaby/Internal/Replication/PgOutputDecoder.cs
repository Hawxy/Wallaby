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
    /// <summary>
    /// Read all columns of a replication tuple, copying values out. Columns named in
    /// <paramref name="utf8JsonColumns"/> are read as raw UTF-8 JSON bytes instead of a decoded string
    /// (see <see cref="Wallaby.Model.CapturedColumn.ReadAsUtf8Json"/>).
    /// </summary>
    public static async Task<RawColumn[]> ReadTupleAsync(
        ReplicationTuple tuple, CancellationToken ct, IReadOnlySet<string>? utf8JsonColumns = null)
    {
        var columns = new RawColumn[tuple.NumColumns];
        var i = 0;
        await foreach (var value in tuple.WithCancellation(ct))
        {
            columns[i++] = await ReadValueAsync(value, utf8JsonColumns, ct);
        }
        return columns;
    }

    private static async ValueTask<RawColumn> ReadValueAsync(
        ReplicationValue value, IReadOnlySet<string>? utf8JsonColumns, CancellationToken ct)
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

        object decoded = utf8JsonColumns?.Contains(columnName) == true
            ? await value.Get<byte[]>(ct)
            : await value.Get(ct);
        return new RawColumn { ColumnName = columnName, Value = decoded };
    }
}
