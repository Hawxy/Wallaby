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
    /// Read all columns of a replication tuple, copying values out. <paramref name="readModes"/> is
    /// aligned to the relation's column order; null means every column reads with
    /// <see cref="ColumnReadMode.Default"/>.
    /// </summary>
    public static async ValueTask<RawColumn[]> ReadTupleAsync(
        ReplicationTuple tuple, ColumnReadMode[]? readModes, CancellationToken ct)
    {
        var columns = new RawColumn[tuple.NumColumns];
        var i = 0;
        await foreach (var value in tuple.WithCancellation(ct))
        {
            var columnName = value.GetFieldName();
            columns[i] = value.IsUnchangedToastedValue
                ? new RawColumn { ColumnName = columnName, Value = null, IsUnchangedToast = true }
                : new RawColumn
                {
                    ColumnName = columnName,
                    Value = await ColumnValueReader.ReadAsync(value, readModes?[i] ?? ColumnReadMode.Default, ct),
                };
            i++;
        }
        return columns;
    }
}
