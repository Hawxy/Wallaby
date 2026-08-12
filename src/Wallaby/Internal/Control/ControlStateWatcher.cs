using Microsoft.Extensions.Logging;
using Wallaby.Client.Internal;

namespace Wallaby.Internal.Control;

/// <summary>
/// Leader-side control watcher: observes the control row (LISTEN + fallback poll) and cancels the leader
/// workload when a suspension is requested (so the session winds down cleanly and releases the slot for
/// the runtime to drop) or when the publication-widening flag flips against the session's baseline, so
/// the next term's bootstrap reconciles the publications to the new width (a plain session bounce: the
/// slot is untouched and checkpoint continuity holds). A transient read failure is logged and retried; it
/// must never fault a healthy streaming session, so unlike the backfill/fan-out tasks this one only ends
/// on cancellation or an observed transition.
/// </summary>
internal sealed class ControlStateWatcher(
    PostgresControlStore store, bool widenedBaseline, TimeSpan pollInterval, ILogger logger)
{
    private volatile bool _suspendObserved;
    private volatile bool _reconfigureObserved;

    /// <summary>True when the session was cancelled because a suspension was observed.</summary>
    public bool SuspendObserved => _suspendObserved;

    /// <summary>
    /// True when the session was cancelled because the publication-widening flag changed; the next
    /// leader term applies the new publication width via its normal reconcile.
    /// </summary>
    public bool ReconfigureObserved => _reconfigureObserved;

    public async Task RunAsync(CancellationTokenSource sessionCts, CancellationToken ct)
    {
        await using var subscription = store.Subscribe();
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var row = await store.ReadAsync(ct);
                if (row is not null && row.State != ControlContract.StateRunning)
                {
                    _suspendObserved = true;
                    logger.SuspendObserved();
                    await sessionCts.CancelAsync();
                    return;
                }
                if ((row?.PublicationsWidened ?? false) != widenedBaseline)
                {
                    _reconfigureObserved = true;
                    logger.WideningChangeObserved(row?.PublicationsWidened ?? false);
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
                // The LISTEN connection is equally unreachable, so WaitAsync would return at once; pace here.
                logger.ControlReadFailed(ex);
                try { await Task.Delay(pollInterval, ct); }
                catch (OperationCanceledException) { return; }
                continue;
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

    [LoggerMessage(Level = LogLevel.Information, Message = "Publication widening flag changed (widened={Widened}); bouncing the leader session to reconcile publication membership. The slot is untouched — no re-backfill.")]
    internal static partial void WideningChangeObserved(this ILogger logger, bool widened);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to read the Wallaby control state; will retry.")]
    internal static partial void ControlReadFailed(this ILogger logger, Exception ex);
}
