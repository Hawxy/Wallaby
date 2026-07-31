using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Client.Internal;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Internal;
using Wallaby.Internal.Control;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;
using Wallaby.Providers;

namespace Wallaby.Hosting;

/// <summary>
/// Provision-only hosted service: when the consumer declares external slots but no capture (no sink or
/// mappings), this creates/reconciles the declared pgoutput publications + slots. There is no primary
/// slot and no streaming. Each provisioning round runs under the cluster lock so only one node
/// provisions at a time, and is idempotent. The service then stays alive watching the control channel:
/// a suspension drops the slots (honored via the gate, never undone by re-provisioning), and each
/// resume re-provisions them. A failure faults the host (which restarts and retries), matching
/// <see cref="WallabyBackgroundService"/>.
/// </summary>
internal sealed class ExternalSlotProvisioningService(
    WallabyConfiguration config,
    WallabyOptions options,
    WallabyDataSource dataSource,
    IClusterLock clusterLock,
    WallabyStatus status,
    IServiceProvider services,
    ILogger<ExternalSlotProvisioningService> logger) : BackgroundService
{
    // All provision-only nodes serialize on this lock (there is no primary slot to key on).
    private const string LockKey = "wallaby_external_slots";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            logger.ProvisioningStarting(WallabyVersion.Current);

            // No external slots declared (e.g. behind a consumer env gate) — do nothing, don't touch the DB.
            if (config.ExternalSlots.Count == 0)
            {
                logger.NoExternalSlots();
                status.MarkStopped();
                return;
            }

            // ForEntity<T>() needs the providers' models; only build them when a slot actually uses an entity type.
            var needsModel = config.ExternalSlots.Exists(s => s.EntityTypes.Count > 0);
            IReadOnlyList<(string Name, IWallabyModelProvider Provider)> modelProviders = needsModel
                ? [.. config.Providers.Select(p => (p.Name, Provider: p.ModelProvider(services)))]
                : [];
            var specs = ExternalSlotResolver.Resolve(config.ExternalSlots, modelProviders);
            var control = new PostgresControlStore(dataSource, options, logger);

            while (!stoppingToken.IsCancellationRequested)
            {
                var snapshot = await WaitOutSuspensionAsync(control, stoppingToken);
                await ProvisionRoundAsync(specs, stoppingToken);

                // Watching: alive so the next suspend/resume cycle re-provisions, holding no lock.
                status.EnterStandby();
                await WaitForControlChangeAsync(control, snapshot, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            status.MarkStopped(); // graceful shutdown
        }
        catch (Exception ex)
        {
            status.MarkFaulted($"{ex.GetType().Name}: {ex.Message}");
            logger.ProvisioningFailed(ex);
            throw;
        }
    }

    /// <summary>
    /// One provisioning round under the cluster lock. A lease held elsewhere is retried rather than
    /// skipped: only a round this node completed proves the slots exist (the other holder may be a
    /// node about to die), and a completed round elsewhere makes ours a fast idempotent reconcile.
    /// </summary>
    private async Task ProvisionRoundAsync(IReadOnlyList<ExternalSlotSpec> specs, CancellationToken ct)
    {
        var waitLogged = false;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            await using var lease = await clusterLock.TryAcquireAsync(LockKey, ct);
            if (lease is not null)
            {
                var configurator = new PostgresSelfConfigurator(
                    dataSource.Source, new SelfConfigOptions { ExternalSlots = specs }, logger);
                await configurator.EnsureExternalSlotsOnlyAsync(ct);
                return;
            }

            if (!waitLogged)
            {
                logger.ProvisioningSkipped();
                waitLogged = true;
            }
            await Task.Delay(options.Advanced.StandbyRetryInterval, ct);
        }
    }

    /// <summary>
    /// Block until the control row differs from <paramref name="snapshot"/> (taken before the
    /// provisioning round), woken by NOTIFY with the poll interval as a safety net. Level-triggered on
    /// the row rather than on observing the Suspended state: every transition stamps a timestamp, so a
    /// suspend/resume cycle faster than any observation still leaves the row changed and triggers a
    /// reconcile round for the slots its finalize dropped. Transient control-read failures are paced
    /// here rather than faulting the host.
    /// </summary>
    private async Task WaitForControlChangeAsync(
        PostgresControlStore control, ControlRow? snapshot, CancellationToken ct)
    {
        await using var subscription = control.Subscribe();
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!Equals(await control.ReadAsync(ct), snapshot))
                {
                    return;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.ControlReadFailed(ex);
                await Task.Delay(options.Advanced.ControlPollInterval, ct);
                continue;
            }

            await subscription.WaitAsync(options.Advanced.ControlPollInterval, ct);
        }
        ct.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// The suspension control gate: finalize a requested suspension (under the lock), and while suspended
    /// idle on the control channel instead of provisioning. Returns the control row that allowed
    /// provisioning (the change-detection snapshot for <see cref="WaitForControlChangeAsync"/>); with a
    /// deployed Suspend() flag that never happens, and the node stays suspended until redeployed.
    /// Transient control-read failures (expected while the database is offline for the upgrade itself)
    /// are retried here rather than faulting the host.
    /// </summary>
    private async Task<ControlRow?> WaitOutSuspensionAsync(PostgresControlStore control, CancellationToken ct)
    {
        INotifySubscription? subscription = null;
        var announced = false;
        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                ControlGateAction gate;
                ControlRow? row;
                try
                {
                    (gate, row) = await ControlGateEvaluator.EvaluateAsync(
                        control, options.Suspended, options.SuspensionReason, logger, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    logger.ControlReadFailed(ex);
                    await Task.Delay(options.Advanced.ControlPollInterval, ct);
                    continue;
                }

                switch (gate)
                {
                    case ControlGateAction.Proceed:
                        return row;

                    case ControlGateAction.Finalize:
                    {
                        await using var lease = await clusterLock.TryAcquireAsync(LockKey, ct);
                        if (lease is not null)
                        {
                            logger.FinalizingSuspension(LockKey);
                            await control.FinalizeSuspensionAsync(TimeSpan.FromSeconds(1), ct);
                        }
                        else
                        {
                            // Another node is finalizing; check back shortly.
                            await Task.Delay(options.Advanced.StandbyRetryInterval, ct);
                        }
                        continue;
                    }

                    default: // Idle
                        if (!announced)
                        {
                            status.EnterSuspended(row?.RequestedAt ?? row?.SuspendedAt, row?.Reason);
                            logger.Suspended(LockKey);
                            announced = true;
                        }
                        subscription ??= control.Subscribe();
                        await subscription.WaitAsync(options.Advanced.ControlPollInterval, ct);
                        continue;
                }
            }
        }
        finally
        {
            if (subscription is not null)
            {
                await subscription.DisposeAsync();
            }
        }
    }
}

/// <summary>Source-generated log messages for <see cref="ExternalSlotProvisioningService"/>.</summary>
internal static partial class ExternalSlotProvisioningServiceLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby {Version} starting in provision-only mode (no capture declared).")]
    internal static partial void ProvisioningStarting(this ILogger logger, string version);

    [LoggerMessage(Level = LogLevel.Information, Message = "No external slots declared; external-slot provisioning is a no-op.")]
    internal static partial void NoExternalSlots(this ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Another node holds the external-slot provisioning lock; skipping this run.")]
    internal static partial void ProvisioningSkipped(this ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "External-slot provisioning failed.")]
    internal static partial void ProvisioningFailed(this ILogger logger, Exception ex);
}
