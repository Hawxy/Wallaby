using Microsoft.Extensions.Logging;
using Npgsql;
using Wallaby.Diagnostics;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Leader-side sampler for the <c>wallaby.slot.retained_wal</c> gauge: the WAL bytes the server
/// retains for the slot (its <c>restart_lsn</c> to the current write position). On a healthy slot the
/// value stays small (the idle heartbeat keeps <c>confirmed_flush_lsn</c> advancing); sustained growth
/// means acknowledgements have stalled and the slot is heading toward <c>max_slot_wal_keep_size</c>
/// invalidation. Samples on the pooled primary-targeted connection; a failed or empty sample (slot not
/// yet created) is skipped and the next tick retries, never ending the leader session.
/// </summary>
internal sealed class SlotLagSampler(
    NpgsqlDataSource dataSource,
    string slotName,
    TimeSpan interval,
    WallabyInstrumentation instrumentation,
    ILogger logger)
{
    public async Task RunAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(interval);
        while (await timer.WaitForNextTickAsync(ct))
        {
            if (!instrumentation.SlotRetainedWalEnabled)
            {
                continue;
            }

            try
            {
                await using var connection = await dataSource.OpenConnectionAsync(ct);
                var bytes = await PgExec.ScalarAsync(
                    connection,
                    """
                    SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
                    FROM pg_replication_slots
                    WHERE slot_name = @s AND restart_lsn IS NOT NULL
                    """,
                    ct, ("s", slotName));
                if (bytes is long value)
                {
                    instrumentation.RecordSlotRetainedWal(slotName, value);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A briefly-unavailable database must not end the leader session; the next tick retries.
                logger.SlotLagSampleFailed(ex);
            }
        }
    }
}

/// <summary>Source-generated log messages for <see cref="SlotLagSampler"/>.</summary>
internal static partial class SlotLagSamplerLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Retained-WAL sampling failed; retrying next tick.")]
    internal static partial void SlotLagSampleFailed(this ILogger logger, Exception ex);
}
