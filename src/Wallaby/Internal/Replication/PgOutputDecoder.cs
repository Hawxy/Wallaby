using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
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
    /// <see cref="ColumnReadMode.Default"/>. <paramref name="relation"/> and <paramref name="walStart"/>
    /// identify the source table and change for decode-failure context.
    /// </summary>
    public static async ValueTask<RawColumn[]> ReadTupleAsync(
        ReplicationTuple tuple, ColumnReadMode[]? readModes, RelationMessage relation,
        NpgsqlLogSequenceNumber walStart, CancellationToken ct)
    {
        var columns = new RawColumn[tuple.NumColumns];
        var i = 0;
        await foreach (var value in tuple.WithCancellation(ct))
        {
            var columnName = value.GetFieldName();
            if (value.IsUnchangedToastedValue)
            {
                columns[i] = new RawColumn { ColumnName = columnName, Value = null, IsUnchangedToast = true };
            }
            else
            {
                object? decoded;
                try
                {
                    decoded = await ColumnValueReader.ReadAsync(value, readModes?[i] ?? ColumnReadMode.Default, ct);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    throw new InvalidOperationException(
                        $"Failed to decode column '{columnName}' of {relation.Namespace}.{relation.RelationName} " +
                        $"at WAL position {walStart}: {ex.Message}", ex);
                }
                columns[i] = new RawColumn { ColumnName = columnName, Value = decoded };
            }
            i++;
        }
        return columns;
    }
}
