using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Internal;
using Wallaby.Internal.Control;
using Wallaby.Internal.SelfConfig;
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

            // ForEntity<T>(), ForAllEntities() and Except<T>() need the providers' models; only build them when a slot uses one.
            var needsModel = config.ExternalSlots.Exists(s => s.NeedsModel);
            IReadOnlyList<(string Name, IWallabyModelProvider Provider)> modelProviders = needsModel
                ? [.. config.Providers.Select(p => (p.Name, Provider: p.ModelProvider(services)))]
                : [];
            var specs = ExternalSlotResolver.Resolve(config.ExternalSlots, modelProviders);
            var gate = new ControlGate(
                new PostgresControlStore(dataSource, options, logger), clusterLock, LockKey, options, status, logger);

            while (!stoppingToken.IsCancellationRequested)
            {
                // A suspension drops the slots (honored via the gate, never undone by re-provisioning);
                // with a deployed Suspend() flag the gate never opens and the node stays suspended.
                var snapshot = await gate.WaitUntilRunningAsync(stoppingToken);
                await ProvisionRoundAsync(specs, stoppingToken);

                // Watching: alive so the next suspend/resume cycle re-provisions, holding no lock.
                status.EnterStandby();
                await gate.WaitForChangeAsync(snapshot, stoppingToken);
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
