using Microsoft.Extensions.Logging;
using Npgsql;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Leader-side idle heartbeat. Since Postgres 15 pgoutput skips empty transactions, so a slot whose
/// mapped tables are quiet receives nothing to acknowledge and pins WAL while other tables churn —
/// until <c>max_slot_wal_keep_size</c> invalidates it. Whenever no transaction has been acknowledged
/// for one interval, this emits a tiny transactional <c>wallaby.heartbeat</c> message on a normal
/// connection; it flows through pgoutput as an empty committed transaction and advances
/// <c>confirmed_flush_lsn</c> through the pipeline's ordinary delivery/ack path.
/// <para>
/// The heartbeat's own acknowledgement registers as progress on the following tick, so the effective
/// idle cadence is between one and two intervals — irrelevant at WAL-retention timescales.
/// </para>
/// </summary>
internal sealed class HeartbeatEmitter(
    NpgsqlDataSource dataSource, Func<ulong> observeAcknowledgedLsn, TimeSpan interval, ILogger logger)
{
    private long _emitted;

    /// <summary>Heartbeats emitted this session. Read cross-thread by tests asserting suppression.</summary>
    internal long EmittedCount => Interlocked.Read(ref _emitted);

    public async Task RunAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(interval);
        var lastSeen = observeAcknowledgedLsn();
        while (await timer.WaitForNextTickAsync(ct))
        {
            var current = observeAcknowledgedLsn();
            if (current != lastSeen)
            {
                lastSeen = current;
                continue;
            }

            try
            {
                await PgExec.ExecuteAsync(
                    dataSource,
                    "SELECT pg_logical_emit_message(true, @prefix, '')", ct,
                    ("prefix", WallabySchema.HeartbeatPrefix));
                Interlocked.Increment(ref _emitted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A briefly-unavailable database must not end the leader session; the next tick retries.
                logger.HeartbeatEmitFailed(ex);
            }
        }
    }
}

internal static partial class HeartbeatEmitterLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Heartbeat emission failed; retrying next tick.")]
    internal static partial void HeartbeatEmitFailed(this ILogger logger, Exception ex);
}
