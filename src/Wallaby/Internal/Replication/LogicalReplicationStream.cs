using System.Runtime.CompilerServices;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using NpgsqlTypes;
using Wallaby.Abstractions;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Wraps an Npgsql <see cref="LogicalReplicationConnection"/> streaming pgoutput from a named slot, and
/// yields fully decoded <see cref="CommittedTransaction"/>s. Acknowledgement of progress is left to the
/// caller (via <see cref="AcknowledgeAsync"/>) so the slot's <c>confirmed_flush_lsn</c> only advances
/// after downstream delivery, preserving at-least-once semantics.
/// </summary>
/// <remarks>
/// We construct a <see cref="LogicalReplicationConnection"/> directly from the supplied connection
/// string — replication connections run in a special protocol mode and cannot be obtained from
/// <see cref="NpgsqlDataSource.OpenConnectionAsync(CancellationToken)"/>, and Npgsql strips the
/// password from <c>NpgsqlDataSource.ConnectionString</c> so we can't reuse it for auth.
/// </remarks>
internal sealed class LogicalReplicationStream(
    string connectionString, string slotName, string publicationName, ITransactionSpill spill,
    int maxBufferedChangesPerTransaction = int.MaxValue) : IAsyncDisposable
{
    private readonly LogicalReplicationConnection _connection = new(connectionString);
    private readonly PgOutputReplicationSlot _slot = new(slotName);
    // Serializes all status-update writes (acks + keepalives) so they never overlap on the connection.
    private readonly SemaphoreSlim _statusLock = new(1, 1);
    // Protocol v2 with streaming: the server streams a transaction larger than its logical_decoding_work_mem
    // before commit (StreamStart/Stop/Commit/Abort), so the assembler can buffer it incrementally rather than
    // the server holding the whole transaction. Requires PG14+ (already our floor via the messages option).
    // Binary mode so Npgsql decodes values to proper CLR types (e.g. DateTime, decimal) rather than text.
    // messages: true asks pgoutput to forward generic WAL messages from pg_logical_emit_message — the
    // transport for backfill low/high watermarks.
    private readonly PgOutputReplicationOptions _options =
        new(publicationName, PgOutputProtocolVersion.V2, binary: true, streamingMode: PgOutputStreamingMode.On, messages: true);

    /// <summary>Stream committed transactions until cancelled.</summary>
    public async IAsyncEnumerable<CommittedTransaction> ReadAsync([EnumeratorCancellation] CancellationToken ct)
    {
        await _connection.Open(ct);
        var assembler = new TransactionAssembler(spill, maxBufferedChangesPerTransaction);

        await foreach (var message in _connection.StartReplication(_slot, _options, ct))
        {
            // Npgsql tracks LastReceivedLsn and answers keepalives internally.
            var transaction = await assembler.ProcessAsync(message, ct);
            if (transaction is not null)
            {
                yield return transaction;
            }
        }
    }

    /// <summary>
    /// Confirm durable processing up to <paramref name="lsn"/> and flush the status to the server,
    /// allowing it to advance <c>confirmed_flush_lsn</c> and recycle WAL.
    /// </summary>
    public async Task AcknowledgeAsync(ulong lsn, CancellationToken ct)
    {
        await _statusLock.WaitAsync(ct);
        try
        {
            _connection.SetReplicationStatus(new NpgsqlLogSequenceNumber(lsn));
            await _connection.SendStatusUpdate(ct);
        }
        finally
        {
            _statusLock.Release();
        }
    }

    /// <summary>
    /// Begin sending periodic status updates so the connection stays alive while a transaction is being
    /// processed — i.e. while the consumer isn't pulling from the stream, so Npgsql can't answer the
    /// server's keepalives. Scope it to one transaction's processing: the replication enumerator is
    /// suspended then (no concurrent socket read), so sending is safe; dispose it before reading resumes.
    /// The update reports the last <see cref="AcknowledgeAsync"/> position (it never calls
    /// <c>SetReplicationStatus</c>), so <c>confirmed_flush_lsn</c> is not advanced past durable delivery.
    /// Disposing stops the ticks and lets an in-flight send finish; cancelling <paramref name="ct"/>
    /// (shutdown/lost lock) also aborts an in-flight send, so teardown can't be blocked by a wedged connection.
    /// </summary>
    public IAsyncDisposable StartKeepalive(TimeSpan interval, CancellationToken ct) => new Keepalive(this, interval, ct);

    private async Task SendKeepaliveAsync(CancellationToken abort)
    {
        await _statusLock.WaitAsync(abort);
        try
        {
            await _connection.SendStatusUpdate(abort);
        }
        finally
        {
            _statusLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
        _statusLock.Dispose();
    }

    private sealed class Keepalive : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly Task _loop;

        public Keepalive(LogicalReplicationStream stream, TimeSpan interval, CancellationToken ct)
        {
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _loop = RunAsync(stream, interval, _cts.Token, ct);
        }

        private static async Task RunAsync(
            LogicalReplicationStream stream, TimeSpan interval, CancellationToken tick, CancellationToken abort)
        {
            try
            {
                using var timer = new PeriodicTimer(interval);
                while (await timer.WaitForNextTickAsync(tick))
                {
                    // The send aborts only on the outer (shutdown/lost-lock) token — a normal dispose
                    // between transactions cancels just the tick wait, so an in-flight send on a healthy
                    // connection is never torn mid-write.
                    await stream.SendKeepaliveAsync(abort);
                }
            }
            catch (OperationCanceledException)
            {
                // Stopped between ticks, or the send was aborted on shutdown.
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _cts.CancelAsync();
            try
            {
                await _loop; // ensure no keepalive send is in flight before reading resumes
            }
            catch
            {
                // The loop swallows cancellation; ignore anything else during teardown.
            }
            _cts.Dispose();
        }
    }
}
