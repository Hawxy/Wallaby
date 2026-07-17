using Microsoft.Extensions.Logging;

namespace Wallaby.Internal.Control;

/// <summary>
/// Leader-side control watcher: observes the control row (LISTEN + fallback poll) and cancels the leader
/// workload when a suspension is requested, so the session winds down cleanly and releases the slot for
/// the runtime to drop. A transient read failure is logged and retried — it must never fault a healthy
/// streaming session — so unlike the backfill/fan-out tasks this one only ends on cancellation or an
/// observed suspension.
/// </summary>
internal sealed class ControlStateWatcher(PostgresControlStore store, TimeSpan pollInterval, ILogger logger)
{
    private volatile bool _suspendObserved;

    /// <summary>True when the session was cancelled because a suspension was observed.</summary>
    public bool SuspendObserved => _suspendObserved;

    public async Task RunAsync(CancellationTokenSource sessionCts, CancellationToken ct)
    {
        await using var subscription = store.Subscribe();
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (await store.IsSuspensionInEffectAsync(ct))
                {
                    _suspendObserved = true;
                    logger.SuspendObserved();
                    await sessionCts.CancelAsync();
                    return;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.ControlReadFailed(ex);
            }

            await subscription.WaitAsync(pollInterval, ct);
        }
    }
}

/// <summary>Source-generated log messages for <see cref="ControlStateWatcher"/>.</summary>
internal static partial class ControlStateWatcherLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby suspension requested; winding down the leader session.")]
    internal static partial void SuspendObserved(this ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to read the Wallaby control state; will retry.")]
    internal static partial void ControlReadFailed(this ILogger logger, Exception ex);
}
