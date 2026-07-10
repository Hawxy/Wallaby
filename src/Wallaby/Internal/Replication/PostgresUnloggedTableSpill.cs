using Npgsql;
using NpgsqlTypes;
using Wallaby.Abstractions;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Disk-free <see cref="ITransactionSpill"/>: buffers a streamed transaction's changes in the
/// <c>wallaby.stream_buffer</c> <b>UNLOGGED</b> table on the source database (so it works wherever Wallaby can
/// connect, with no local disk). UNLOGGED means the buffer writes generate no WAL (no slot feedback, no
/// re-capture) and are auto-truncated on crash. Engages only for streamed (large) transactions, so the extra
/// source I/O is paid only then. Appends are buffered to a small bounded window and flushed via binary COPY.
/// </summary>
internal sealed class PostgresUnloggedTableSpill(NpgsqlDataSource dataSource, string slotName) : ITransactionSpill
{
    private const int FlushThreshold = 500;

    private readonly Dictionary<uint, List<(uint Subxid, RawChange Change)>> _pending = [];
    private readonly Dictionary<uint, long> _nextSeq = [];

    public async ValueTask AppendAsync(uint xid, uint subxid, RawChange change, CancellationToken ct)
    {
        if (!_pending.TryGetValue(xid, out var batch))
        {
            batch = [];
            _pending[xid] = batch;
        }
        batch.Add((subxid, change));
        if (batch.Count >= FlushThreshold)
        {
            await FlushAsync(xid, ct);
        }
    }

    public async IAsyncEnumerable<RawChange> ReadAsync(
        uint xid, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await FlushAsync(xid, ct);

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT payload FROM wallaby.stream_buffer WHERE slot_name = @s AND xid = @x ORDER BY seq", connection);
        cmd.Parameters.AddWithValue("s", slotName);
        cmd.Parameters.AddWithValue("x", (long)xid);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            yield return SpillCodec.Decode(reader.GetFieldValue<byte[]>(0));
        }
    }

    public async ValueTask DiscardAsync(uint xid, CancellationToken ct)
    {
        _pending.Remove(xid);
        _nextSeq.Remove(xid);
        await PgExec.ExecuteAsync(
            dataSource, "DELETE FROM wallaby.stream_buffer WHERE slot_name = @s AND xid = @x", ct,
            ("s", slotName), ("x", (long)xid));
    }

    public async ValueTask DiscardSubtransactionAsync(uint xid, uint subxid, CancellationToken ct)
    {
        // Truncate flushed rows from the subtransaction's first change onward; min(seq) is NULL (deleting
        // nothing) when the subxid never flushed.
        var deleted = await PgExec.ExecuteAsync(
            dataSource,
            "DELETE FROM wallaby.stream_buffer WHERE slot_name = @s AND xid = @x AND seq >= " +
            "(SELECT min(seq) FROM wallaby.stream_buffer WHERE slot_name = @s AND xid = @x AND subxid = @sub)",
            ct, ("s", slotName), ("x", (long)xid), ("sub", (long)subxid));

        if (!_pending.TryGetValue(xid, out var batch))
        {
            return;
        }

        if (deleted > 0)
        {
            // A flush drains the whole batch, so everything still pending was appended after the flushed
            // first change — it all goes, even entries carrying only descendant subxids.
            batch.Clear();
            return;
        }

        var first = batch.FindIndex(e => e.Subxid == subxid);
        if (first >= 0)
        {
            batch.RemoveRange(first, batch.Count - first);
        }
    }

    public async ValueTask ClearAsync(CancellationToken ct)
    {
        _pending.Clear();
        _nextSeq.Clear();
        await PgExec.ExecuteAsync(
            dataSource, "DELETE FROM wallaby.stream_buffer WHERE slot_name = @s", ct, ("s", slotName));
    }

    public ValueTask DisposeAsync()
    {
        _pending.Clear();
        _nextSeq.Clear();
        return ValueTask.CompletedTask;
    }

    private async Task FlushAsync(uint xid, CancellationToken ct)
    {
        if (!_pending.TryGetValue(xid, out var batch) || batch.Count == 0)
        {
            return;
        }

        var seq = _nextSeq.GetValueOrDefault(xid);
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using (var importer = await connection.BeginBinaryImportAsync(
            "COPY wallaby.stream_buffer (slot_name, xid, subxid, seq, payload) FROM STDIN (FORMAT BINARY)", ct))
        {
            foreach (var (subxid, change) in batch)
            {
                await importer.StartRowAsync(ct);
                await importer.WriteAsync(slotName, NpgsqlDbType.Text, ct);
                await importer.WriteAsync((long)xid, NpgsqlDbType.Bigint, ct);
                await importer.WriteAsync((long)subxid, NpgsqlDbType.Bigint, ct);
                await importer.WriteAsync(seq++, NpgsqlDbType.Bigint, ct);
                await importer.WriteAsync(SpillCodec.Encode(change), NpgsqlDbType.Bytea, ct);
            }
            await importer.CompleteAsync(ct);
        }

        _nextSeq[xid] = seq;
        batch.Clear();
    }
}
