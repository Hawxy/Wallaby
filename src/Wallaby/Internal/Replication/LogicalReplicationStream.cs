using System.Runtime.CompilerServices;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using NpgsqlTypes;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Wraps an Npgsql <see cref="LogicalReplicationConnection"/> streaming pgoutput from a named slot, and
/// yields fully decoded <see cref="CommittedTransaction"/>s. Acknowledgement of progress is left to the
/// caller (via <see cref="AcknowledgeAsync"/>) so the slot's <c>confirmed_flush_lsn</c> only advances
/// after downstream delivery, preserving at-least-once semantics.
/// </summary>
/// <remarks>
/// We construct a <see cref="LogicalReplicationConnection"/> directly from the supplied connection
/// string: replication connections run in a special protocol mode and cannot be obtained from
/// <see cref="NpgsqlDataSource.OpenConnectionAsync(CancellationToken)"/>, and Npgsql strips the
/// password from <c>NpgsqlDataSource.ConnectionString</c> so we can't reuse it for auth.
/// </remarks>
internal sealed class LogicalReplicationStream(
    string connectionString, string slotName, string publicationName, ITransactionSpill spill,
    int maxBufferedChangesPerTransaction = int.MaxValue, WallabyModel? model = null,
    WallabyInstrumentation? instrumentation = null) : IAsyncDisposable
{
    private readonly LogicalReplicationConnection _connection = new(WithArrayNullability(connectionString));
    private readonly PgOutputReplicationSlot _slot = new(slotName);
    // Serializes all status-update writes (acks + keepalives) so they never overlap on the connection.
    private readonly SemaphoreSlim _statusLock = new(1, 1);
    // Protocol v2 with streaming: the server streams a transaction larger than its logical_decoding_work_mem
    // before commit (StreamStart/Stop/Commit/Abort), so the assembler can buffer it incrementally rather than
    // the server holding the whole transaction. Well within the PG15 floor (enforced by ServerValidator).
    // Binary mode so Npgsql decodes values to proper CLR types (e.g. DateTime, decimal) rather than text.
    // messages: true asks pgoutput to forward generic WAL messages from pg_logical_emit_message, the
    // transport for backfill low/high watermarks.
    private readonly PgOutputReplicationOptions _options =
        new(publicationName, PgOutputProtocolVersion.V2, binary: true, streamingMode: PgOutputStreamingMode.On, messages: true);

    /// <summary>Stream committed transactions until cancelled.</summary>
    public async IAsyncEnumerable<CommittedTransaction> ReadAsync([EnumeratorCancellation] CancellationToken ct)
    {
        await _connection.Open(ct);
        var assembler = new TransactionAssembler(spill, maxBufferedChangesPerTransaction, model, slotName, instrumentation);

        var cleared = false;
        await foreach (var message in _connection.StartReplication(_slot, _options, ct))
        {
            if (!cleared)
            {
                // A received message proves this node exclusively holds the slot (START_REPLICATION fails
                // while another node streams from it), so stale spill rows can't belong to a live leader.
                // Clearing before the first ProcessAsync keeps a re-streamed transaction (same xid as a
                // crashed run) from appending onto that run's leftovers.
                await spill.ClearAsync(ct);
                cleared = true;
            }

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
            await SendStatusUpdateGuardedAsync(ct);
        }
        finally
        {
            _statusLock.Release();
        }
    }

    /// <summary>
    /// Create the keepalive guard for a pipeline run: a single timer loop that sends a status update on
    /// each tick falling inside a <see cref="KeepaliveGuard.BeginTransaction"/>/<see
    /// cref="KeepaliveGuard.EndTransactionAsync"/> window, i.e. while a batch is being processed,
    /// when the consumer isn't pulling from the stream and Npgsql can't answer the server's keepalives.
    /// The update reports the last <see cref="AcknowledgeAsync"/> position (it never calls
    /// <c>SetReplicationStatus</c>), so <c>confirmed_flush_lsn</c> is not advanced past durable delivery.
    /// <paramref name="readInFlight"/> reports whether the batcher left a stream read in flight; Npgsql
    /// answers the server's keepalives itself while reading, so the guard skips those ticks.
    /// Cancelling <paramref name="abort"/> (shutdown/lost lock) aborts an in-flight send, so teardown
    /// can't be blocked by a wedged connection.
    /// </summary>
    public KeepaliveGuard StartKeepalive(TimeSpan interval, CancellationToken abort, Func<bool>? readInFlight = null)
        => new(this, interval, abort, readInFlight);

    private async Task SendKeepaliveAsync(CancellationToken abort)
    {
        await _statusLock.WaitAsync(abort);
        try
        {
            await SendStatusUpdateGuardedAsync(abort);
        }
        finally
        {
            _statusLock.Release();
        }
    }

    /// <summary>
    /// Send a status update, translating the exceptions Npgsql leaks when the replication stream
    /// terminates concurrently with the send. Npgsql 10.0.3's <c>SendFeedback</c> swallows every
    /// exception from the send itself (including cancellation), then re-arms its status timer with a
    /// null-forgiving dereference in a finally block; the replication enumerator's teardown disposes
    /// and nulls that timer without synchronizing with an in-flight send. A send overlapping a
    /// pending stream read (the batcher's in-flight read, see <see cref="KeepaliveGuard"/>) can
    /// therefore surface <see cref="NullReferenceException"/> or
    /// <see cref="ObjectDisposedException"/> when that read observes cancellation or a connection
    /// fault and terminates the enumerator. Streaming has always begun by the time this is called,
    /// so those exceptions prove the stream terminated: report cancellation when the token is
    /// cancelled, otherwise a descriptive failure (the underlying fault, if any, surfaces on the
    /// next stream read).
    /// </summary>
    private async Task SendStatusUpdateGuardedAsync(CancellationToken ct)
    {
        try
        {
            await _connection.SendStatusUpdate(ct);
        }
        catch (Exception ex) when (ex is NullReferenceException or ObjectDisposedException)
        {
            ct.ThrowIfCancellationRequested();
            throw new InvalidOperationException("Replication stream terminated during a status update.", ex);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
        _statusLock.Dispose();
    }

    // NULL array elements throw at decode under Npgsql's default ArrayNullabilityMode.Never; PerInstance
    // decodes them as Nullable<T>[] instead. Applied unless the consumer configured the mode explicitly.
    // Npgsql rejects a multi-host replication connection outright, so a multi-host string must be
    // resolved to the primary first; see ReplicationPrimaryResolver.
    private static string WithArrayNullability(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (!builder.ShouldSerialize("Array Nullability Mode"))
        {
            builder.ArrayNullabilityMode = ArrayNullabilityMode.PerInstance;
        }
        return builder.ConnectionString;
    }

    /// <summary>
    /// One long-lived timer loop per pipeline run: the per-batch hot path is just a flag write on
    /// begin and an uncontended gate acquire on end, with no timer/task churn per batch. The guard's
    /// sends stay out of windows where the enumerator is reading: the Begin/End bracket covers the
    /// processing side, and the <c>readInFlight</c> probe covers a batcher read left pending across a
    /// flush (Npgsql answers the server's keepalives itself during reads, so no guard send is needed
    /// then). The one deliberate overlap is the pipeline's batch ack while such a read is pending: the
    /// same <c>SendFeedback</c> path Npgsql's own <c>WalReceiverStatusInterval</c> timer (default 10s,
    /// active in every deployment) already exercises concurrently with reads, with all feedback writers
    /// serialized inside Npgsql. The write path is safe, but stream termination during the overlap is
    /// not: the pending read tearing down the enumerator races the send's timer re-arm (see
    /// <see cref="SendStatusUpdateGuardedAsync"/>); re-verify both on Npgsql upgrades.
    /// </summary>
    internal sealed class KeepaliveGuard : IAsyncDisposable
    {
        private readonly LogicalReplicationStream _stream;
        private readonly CancellationToken _abort;
        private readonly PeriodicTimer _timer;
        private readonly Task _loop;
        private readonly Func<bool>? _readInFlight;
        // Serializes keepalive sends against EndTransactionAsync, so processing never hands the
        // connection back to the enumerator while a send is in flight.
        private readonly SemaphoreSlim _gate = new(1, 1);
        private volatile bool _processing;

        internal KeepaliveGuard(
            LogicalReplicationStream stream, TimeSpan interval, CancellationToken abort, Func<bool>? readInFlight = null)
        {
            _stream = stream;
            _abort = abort;
            _readInFlight = readInFlight;
            _timer = new PeriodicTimer(interval);
            _loop = RunAsync();
        }

        /// <summary>Mark a transaction as being processed: ticks now send status updates.</summary>
        public void BeginTransaction() => _processing = true;

        /// <summary>
        /// Mark processing finished. A barrier: on return no send is in flight and none will start until
        /// the next <see cref="BeginTransaction"/>, so the caller can resume reading the stream. An
        /// in-flight send on a healthy connection is never torn mid-write; on abort it is cancelled and
        /// this returns (the caller's next stream operation observes the same token).
        /// </summary>
        public async ValueTask EndTransactionAsync()
        {
            _processing = false;
            try
            {
                await _gate.WaitAsync(_abort);
                _gate.Release();
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task RunAsync()
        {
            try
            {
                while (await _timer.WaitForNextTickAsync(_abort))
                {
                    // Skip while a batcher read is in flight; Npgsql answers keepalives during reads.
                    if (!_processing || _readInFlight?.Invoke() is true)
                    {
                        continue;
                    }

                    await _gate.WaitAsync(_abort);
                    try
                    {
                        // Re-check under the gate: EndTransactionAsync may have won it in between, and
                        // the enumerator may be reading again; sending now would race the socket read.
                        if (_processing && _readInFlight?.Invoke() is not true)
                        {
                            await _stream.SendKeepaliveAsync(_abort);
                        }
                    }
                    catch (Exception) when (!_abort.IsCancellationRequested)
                    {
                        // A failed send means the connection is broken; the pipeline's next read/ack
                        // surfaces it. Keep ticking rather than silently dying for the rest of the run.
                    }
                    finally
                    {
                        _gate.Release();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Stopped between ticks, or the send was aborted on shutdown.
            }
        }

        public async ValueTask DisposeAsync()
        {
            _timer.Dispose(); // pending and future ticks return false, so the loop exits cleanly
            try
            {
                await _loop; // ensure no keepalive send is in flight before teardown proceeds
            }
            catch
            {
                // The loop swallows cancellation; ignore anything else during teardown.
            }
            _gate.Dispose();
        }
    }
}
