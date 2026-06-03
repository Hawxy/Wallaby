using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Internal;
using Wallaby.Internal.SelfConfig;

namespace Wallaby.Hosting;

/// <summary>
/// Provision-only hosted service: when the consumer declares external slots but no capture (no sink or
/// mappings), this creates/reconciles the declared pgoutput publications + slots and then completes. There
/// is no primary slot and no streaming. Runs under the cluster lock so only one node provisions at a time,
/// and is idempotent. A failure faults the host (which restarts and retries), matching
/// <see cref="CdcBackgroundService{TContext}"/> and the hand-rolled initializer it replaces.
/// </summary>
internal sealed class ExternalSlotProvisioningService(
    CdcConfiguration config,
    CdcDataSource dataSource,
    IClusterLock clusterLock,
    CdcStatus status,
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

            // ForEntity<T>() needs the EF model; only resolve it when a slot actually uses an entity type.
            var needsModel = config.ExternalSlots.Exists(s => s.EntityTypes.Count > 0);
            var model = needsModel ? config.ModelAccessor?.Invoke(services) : null;
            var specs = ExternalSlotResolver.Resolve(config.ExternalSlots, model);

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
