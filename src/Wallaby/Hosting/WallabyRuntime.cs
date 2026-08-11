using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Client.Internal;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Internal;
using Wallaby.Internal.Control;

namespace Wallaby.Hosting;

/// <summary>
/// The leadership election loop for a Wallaby instance: acquire the cluster lock, run a
/// <see cref="LeaderSession"/> for the lifetime of leadership, and re-elect (with backoff on faults) when
/// it ends. Standby nodes wait and take over on failover. The session's long-lived components are wired
/// once via <see cref="WallabyComponents"/>.
/// </summary>
internal sealed class WallabyRuntime
{
    private readonly ResolvedProviderSet _providers;
    private readonly WallabyConfiguration _config;
    private readonly WallabyOptions _options;
    private readonly WallabyDataSource _dataSource;
    private readonly IClusterLock _clusterLock;
    private readonly IServiceProvider _services;
    private readonly WallabyInstrumentation _instrumentation;
    private readonly WallabyStatus _status;
    private readonly ILogger<WallabyRuntime> _logger;
    private readonly PostgresControlStore _control;

    public WallabyRuntime(
        ResolvedProviderSet providers,
        WallabyConfiguration config,
        WallabyOptions options,
        WallabyDataSource dataSource,
        IClusterLock clusterLock,
        IServiceProvider services,
        WallabyInstrumentation instrumentation,
        WallabyStatus status,
        ILogger<WallabyRuntime> logger)
    {
        _providers = providers;
        _config = config;
        _options = options;
        _dataSource = dataSource;
        _clusterLock = clusterLock;
        _services = services;
        _instrumentation = instrumentation;
        _status = status;
        _logger = logger;
        _control = new PostgresControlStore(dataSource, options, logger);
    }

    // How long to wait between drop attempts when a managed slot is still held by an active consumer.
    private static readonly TimeSpan FinalizeBusyRetryDelay = TimeSpan.FromSeconds(1);

    // A leader session lasting at least this long before failing retries at the base delay (resets backoff);
    // a faster failure (e.g. self-config erroring) grows the backoff so it doesn't hot-loop.
    private static readonly TimeSpan HealthyLeaderSession = TimeSpan.FromMinutes(1);

    public async Task RunAsync(CancellationToken ct)
    {
        _logger.RuntimeStarting(WallabyVersion.Current, _options.SlotName, _options.PublicationName);

        // Disposed when the election loop exits (shutdown or fault), releasing the sinks it materialized.
        await using var components = WallabyComponents.Build(
            _providers, _config, _options, _dataSource, _services, _instrumentation, _status, _logger);

        // Grows the retry delay (with jitter, capped) when leadership acquisition or a leader session keeps
        // failing, so a persistent error backs off instead of spamming at a fixed interval.
        var backoff = new RetryBackoff(_options.Advanced.LeaderRetryInterval);

        while (!ct.IsCancellationRequested)
        {
            // Suspension gate: reconcile the deployed Suspend() flag and honor any suspension before
            // touching the cluster lock or slots.
            try
            {
                var (gate, row) = await ControlGateEvaluator.EvaluateAsync(
                    _control, _options.Suspended, _options.SuspensionReason, _logger, ct);
                _status.SetPublicationsWidened(row?.PublicationsWidened ?? false, row?.WidenedAt);
                if (gate == ControlGateAction.Finalize)
                {
                    await TryFinalizeSuspensionAsync(ct);
                    continue;
                }
                if (gate == ControlGateAction.Idle)
                {
                    await IdleWhileSuspendedAsync(row, ct);
                    continue;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.ControlGateFailed(ex);
                await DelaySafeAsync(backoff.Next(), ct);
                continue;
            }

            IClusterLockHandle? leadership = null;
            try
            {
                leadership = await _clusterLock.TryAcquireAsync(_options.SlotName, ct);
            }
            catch (Exception ex)
            {
                _logger.LeadershipAcquireFailed(ex);
                await DelaySafeAsync(backoff.Next(), ct);
                continue;
            }

            if (leadership is null)
            {
                // The lock is reachable and held by another node — a healthy standby, not an error.
                backoff.Reset();
                _status.EnterStandby();
                _status.ResetLeaderFailures();
                _status.ResetFanoutFailures();
                _logger.Standby(_options.SlotName);
                await DelaySafeAsync(_options.Advanced.StandbyRetryInterval, ct);
                continue;
            }

            await using (leadership)
            {
                _logger.LeadershipAcquired(_options.SlotName);
                _status.EnterLeader(DateTimeOffset.UtcNow);
                var sessionStart = Stopwatch.GetTimestamp();
                try
                {
                    var session = new LeaderSession(
                        components, _config, _options, _dataSource, leadership, _services,
                        _instrumentation, _status);
                    var outcome = await session.RunAsync(ct);
                    backoff.Reset();
                    _status.ResetLeaderFailures();
                    _status.ResetFanoutFailures();
                    if (outcome == LeaderSessionOutcome.SuspendRequested)
                    {
                        // The session released the slot and we still hold the cluster lock, so nothing
                        // else can be streaming it; drop the managed slots and mark the suspension
                        // finalized. The next loop iteration idles on the result.
                        _logger.FinalizingSuspension(_options.SlotName);
                        await _control.FinalizeSuspensionAsync(FinalizeBusyRetryDelay, ct);
                    }
                    else if (outcome == LeaderSessionOutcome.LeadershipLost)
                    {
                        // We stepped down because the lock dropped (not an error); re-elect immediately.
                        _logger.LeadershipLost(_options.SlotName);
                    }
                    else if (outcome == LeaderSessionOutcome.Reconfigure)
                    {
                        // The widening flag flipped: re-elect immediately so the next term's bootstrap
                        // reconciles the publications. The slot was never touched — no re-backfill.
                        _logger.ReconfiguringPublications(_options.SlotName);
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LeaderSessionFailed(ex);
                    _status.RecordLeaderFailure(Describe(ex));
                    if (Stopwatch.GetElapsedTime(sessionStart) >= HealthyLeaderSession)
                    {
                        // A long session that then dropped is likely transient — retry at the base delay.
                        // The failure counter clears only on real progress or a clean step-down.
                        backoff.Reset();
                    }
                    await DelaySafeAsync(backoff.Next(), ct);
                }
            }
        }
    }

    /// <summary>
    /// A suspension was requested with no leader session of our own to wind down. Only the cluster-lock
    /// holder may drop slots: if the lock is free, take it and finalize; if a live leader holds it, its
    /// own control watcher is winding it down to finalize — check back shortly.
    /// </summary>
    private async Task TryFinalizeSuspensionAsync(CancellationToken ct)
    {
        var leadership = await _clusterLock.TryAcquireAsync(_options.SlotName, ct);
        if (leadership is null)
        {
            await DelaySafeAsync(_options.Advanced.StandbyRetryInterval, ct);
            return;
        }

        await using (leadership)
        {
            _logger.FinalizingSuspension(_options.SlotName);
            await _control.FinalizeSuspensionAsync(FinalizeBusyRetryDelay, ct);
        }
    }

    /// <summary>
    /// Suspension idle: hold no lock (so any actor can finalize or resume) and re-run the control gate on
    /// every pass, woken by NOTIFY with the poll interval as a safety net. Re-evaluating (rather than
    /// only polling the state) is what keeps a flag-carrying node's assertion heartbeat fresh, and what
    /// makes a flag-less node whose auto-resume was refused retry until the grace elapses. Exits on any
    /// non-idle gate outcome or shutdown; the main loop re-evaluates and acts on it.
    /// </summary>
    private async Task IdleWhileSuspendedAsync(ControlRow? row, CancellationToken ct)
    {
        _status.EnterSuspended(row?.RequestedAt ?? row?.SuspendedAt, row?.Reason);
        if (!_options.Suspended && row?.Origin == ControlContract.OriginConfiguration)
        {
            // Not "suspended until an explicit resume": this node will resume itself once the
            // flag-carrying nodes' assertion heartbeat goes stale.
            _logger.SuspendedAwaitingGrace(_options.SlotName);
        }
        else
        {
            _logger.Suspended(_options.SlotName);
        }
        await using var subscription = _control.Subscribe();
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var (gate, _) = await ControlGateEvaluator.EvaluateAsync(
                    _control, _options.Suspended, _options.SuspensionReason, _logger, ct);
                if (gate != ControlGateAction.Idle)
                {
                    if (gate == ControlGateAction.Proceed)
                    {
                        _logger.SuspensionEnded(_options.SlotName);
                    }
                    return;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // Expected while the database is offline for the upgrade itself; pace the retries
                // instead of hot-looping on the (equally unreachable) LISTEN connection.
                _logger.ControlReadFailed(ex);
                await DelaySafeAsync(_options.Advanced.ControlPollInterval, ct);
                continue;
            }

            await subscription.WaitAsync(_options.Advanced.ControlPollInterval, ct);
        }
    }

    private async Task DelaySafeAsync(TimeSpan delay, CancellationToken ct)
    {
        // A non-positive value (reachable via post-validation configuration) would throw or spin; floor it.
        if (delay <= TimeSpan.Zero)
        {
            delay = TimeSpan.FromSeconds(1);
        }
        try { await Task.Delay(delay, ct); }
        catch (OperationCanceledException) { }
    }

    private static string Describe(Exception ex) => $"{ex.GetType().Name}: {ex.Message}";
}

/// <summary>Source-generated log messages for <see cref="WallabyRuntime"/>.</summary>
internal static partial class WallabyRuntimeLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to acquire Wallaby leadership; retrying.")]
    internal static partial void LeadershipAcquireFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Wallaby standby: another node holds leadership for slot {Slot}.")]
    internal static partial void Standby(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby {Version} starting for slot {Slot} (publication {Publication}).")]
    internal static partial void RuntimeStarting(this ILogger logger, string version, string slot, string publication);

    [LoggerMessage(Level = LogLevel.Information, Message = "Acquired Wallaby leadership for slot {Slot}.")]
    internal static partial void LeadershipAcquired(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Lost Wallaby leadership for slot {Slot} (lock connection dropped); stepping down and re-electing.")]
    internal static partial void LeadershipLost(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Error, Message = "Wallaby leader session failed; will retry.")]
    internal static partial void LeaderSessionFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to evaluate the Wallaby suspension state; retrying.")]
    internal static partial void ControlGateFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Finalizing Wallaby suspension for slot {Slot}: dropping every managed replication slot.")]
    internal static partial void FinalizingSuspension(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Wallaby is suspended (slot {Slot}): managed replication slots are dropped and streaming is stopped until an explicit resume.")]
    internal static partial void Suspended(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Wallaby is suspended (slot {Slot}) by nodes still deployed with Suspend(); this flag-less node is waiting out the configuration-suspension grace and will auto-resume once their assertion goes stale.")]
    internal static partial void SuspendedAwaitingGrace(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby suspension ended; re-entering leader election for slot {Slot}. Expect a full re-backfill of all mapped tables.")]
    internal static partial void SuspensionEnded(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Information, Message = "Publication widening flag changed; re-entering leader election for slot {Slot} to reconcile publication membership.")]
    internal static partial void ReconfiguringPublications(this ILogger logger, string slot);
}
