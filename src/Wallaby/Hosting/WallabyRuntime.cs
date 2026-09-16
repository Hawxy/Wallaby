using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
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
    private readonly ControlGate _gate;

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
        _gate = new ControlGate(
            new PostgresControlStore(dataSource, options, logger), clusterLock, options.SlotName, options, status, logger);
    }

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
                await _gate.WaitUntilRunningAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
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
                _status.EnterStandby(); // a standby claims no failure streaks; the transition clears them
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
                    _status.ResetFailureStreaks();
                    if (outcome == LeaderSessionOutcome.SuspendRequested)
                    {
                        // The session released the slot and we still hold the cluster lock, so nothing
                        // else can be streaming it; drop the managed slots and mark the suspension
                        // finalized. The next loop iteration idles on the result.
                        await _gate.FinalizeSuspensionAsync(ct);
                    }
                    else if (outcome == LeaderSessionOutcome.LeadershipLost)
                    {
                        // We stepped down because the lock dropped (not an error); re-elect immediately.
                        _logger.LeadershipLost(_options.SlotName);
                    }
                    else if (outcome == LeaderSessionOutcome.Reconfigure)
                    {
                        // The widening flag flipped: re-elect immediately so the next term's bootstrap
                        // reconciles the publications. The slot was never touched, so no re-backfill.
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

    [LoggerMessage(Level = LogLevel.Information, Message = "Publication widening flag changed; re-entering leader election for slot {Slot} to reconcile publication membership.")]
    internal static partial void ReconfiguringPublications(this ILogger logger, string slot);
}
