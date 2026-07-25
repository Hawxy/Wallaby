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
/// mappings), this creates/reconciles the declared pgoutput publications + slots and then completes. There
/// is no primary slot and no streaming. Runs under the cluster lock so only one node provisions at a time,
/// and is idempotent. Honors the suspension control gate before provisioning — a suspension must not be
/// undone by recreating external slots (once this service has completed and exited, a suspension is
/// finalized by the requesting client instead). A failure faults the host (which restarts and retries),
/// matching <see cref="WallabyBackgroundService"/> and the hand-rolled initializer it replaces.
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
            // No external slots declared (e.g. behind a consumer env gate) — do nothing, don't touch the DB.
            if (config.ExternalSlots.Count == 0)
            {
                logger.NoExternalSlots();
                status.MarkStopped();
                return;
            }

            await WaitOutSuspensionAsync(stoppingToken);

            // ForEntity<T>() needs the providers' models; only build them when a slot actually uses an entity type.
            var needsModel = config.ExternalSlots.Exists(s => s.EntityTypes.Count > 0);
            IReadOnlyList<(string Name, IWallabyModelProvider Provider)> modelProviders = needsModel
                ? [.. config.Providers.Select(p => (p.Name, Provider: p.ModelProvider(services)))]
                : [];
            var specs = ExternalSlotResolver.Resolve(config.ExternalSlots, modelProviders);

            await using var lease = await clusterLock.TryAcquireAsync(LockKey, stoppingToken);
            if (lease is null)
            {
                // Another node is provisioning; it owns the work. Idempotent re-runs on the next deploy converge.
                logger.ProvisioningSkipped();
                status.MarkStopped();
                return;
            }

            var configurator = new PostgresSelfConfigurator(
                dataSource.Source, new SelfConfigOptions { ExternalSlots = specs }, logger);
            await configurator.EnsureExternalSlotsOnlyAsync(stoppingToken);
            status.MarkStopped();
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
    /// The suspension control gate: finalize a requested suspension (under the lock), and while suspended
    /// idle on the control channel instead of provisioning. Returns when the state allows provisioning;
    /// with a deployed Suspend() flag that never happens — the node stays suspended until redeployed.
    /// Transient control-read failures (expected while the database is offline for the upgrade itself)
    /// are retried here rather than faulting the host.
    /// </summary>
    private async Task WaitOutSuspensionAsync(CancellationToken ct)
    {
        var control = new PostgresControlStore(dataSource, options, logger);
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
                        return;

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
    [LoggerMessage(Level = LogLevel.Information, Message = "No external slots declared; external-slot provisioning is a no-op.")]
    internal static partial void NoExternalSlots(this ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Another node holds the external-slot provisioning lock; skipping this run.")]
    internal static partial void ProvisioningSkipped(this ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "External-slot provisioning failed.")]
    internal static partial void ProvisioningFailed(this ILogger logger, Exception ex);
}
