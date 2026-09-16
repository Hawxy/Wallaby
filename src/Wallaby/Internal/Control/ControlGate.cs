using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Client.Internal;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Internal.State;

namespace Wallaby.Internal.Control;

/// <summary>
/// The suspend/resume gate every hosted service passes before touching slots: evaluates the control
/// state, finalizes a requested suspension under the cluster lock, and idles on the control channel
/// (NOTIFY, with the poll interval as a safety net) while suspended. Re-evaluating on every idle pass keeps
/// a flag-carrying node's assertion heartbeat fresh and lets a flag-less node retry its auto-resume until
/// the grace elapses. Transient control failures, expected while the database is offline for the upgrade
/// itself, are logged and paced rather than faulting the host.
/// </summary>
internal sealed class ControlGate(
    PostgresControlStore control,
    IClusterLock clusterLock,
    string lockKey,
    WallabyOptions options,
    WallabyStatus status,
    ILogger logger)
{
    // How long to wait between drop attempts when a managed slot is still held by an active consumer.
    private static readonly TimeSpan FinalizeBusyRetryDelay = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Block until the control state allows work, returning the row observed (the change-detection
    /// snapshot for <see cref="WaitForChangeAsync"/>). With a deployed <c>Suspend()</c> flag this never
    /// returns, and the node stays suspended until redeployed.
    /// </summary>
    public async Task<ControlRow?> WaitUntilRunningAsync(CancellationToken ct)
    {
        INotifySubscription? subscription = null;
        var idling = false;
        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    var (gate, row) = await ControlGateEvaluator.EvaluateAsync(
                        control, options.Suspended, options.SuspensionReason, logger, ct);
                    status.SetPublicationsWidened(row?.PublicationsWidened ?? false, row?.WidenedAt);

                    switch (gate)
                    {
                        case ControlGateAction.Proceed:
                            if (idling)
                            {
                                logger.SuspensionEnded(lockKey);
                            }
                            return row;

                        case ControlGateAction.Finalize:
                            idling = false;
                            await using (var lease = await clusterLock.TryAcquireAsync(lockKey, ct))
                            {
                                if (lease is not null)
                                {
                                    await FinalizeSuspensionAsync(ct);
                                    continue;
                                }
                            }
                            // Another node is finalizing; check back shortly.
                            await DelayAsync(options.Advanced.StandbyRetryInterval, ct);
                            continue;

                        default: // Idle
                            if (!idling)
                            {
                                idling = true;
                                status.EnterSuspended(row?.RequestedAt ?? row?.SuspendedAt, row?.Reason);
                                if (!options.Suspended && row?.Origin == ControlContract.OriginConfiguration)
                                {
                                    // Not "suspended until an explicit resume": this node resumes itself once
                                    // the flag-carrying nodes' assertion heartbeat goes stale.
                                    logger.SuspendedAwaitingGrace(lockKey);
                                }
                                else
                                {
                                    logger.Suspended(lockKey);
                                }
                            }
                            subscription ??= control.Subscribe();
                            await subscription.WaitAsync(options.Advanced.ControlPollInterval, ct);
                            continue;
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // The LISTEN connection is equally unreachable, so WaitAsync would return at once; pace here.
                    logger.ControlReadFailed(ex);
                    await DelayAsync(options.Advanced.ControlPollInterval, ct);
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

    /// <summary>
    /// Drop every managed replication slot and mark the suspension finalized. The caller holds the
    /// cluster lock, so nothing else can be streaming the slots.
    /// </summary>
    public Task FinalizeSuspensionAsync(CancellationToken ct)
    {
        logger.FinalizingSuspension(lockKey);
        return control.FinalizeSuspensionAsync(FinalizeBusyRetryDelay, ct);
    }

    /// <summary>
    /// Block until the control row differs from <paramref name="snapshot"/>, woken by NOTIFY with the poll
    /// interval as a safety net. Level-triggered on the row rather than on observing the Suspended state:
    /// every transition stamps a timestamp, so a suspend/resume cycle faster than any observation still
    /// leaves the row changed.
    /// </summary>
    public async Task WaitForChangeAsync(ControlRow? snapshot, CancellationToken ct)
    {
        await using var subscription = control.Subscribe();
        while (true)
        {
            ct.ThrowIfCancellationRequested();
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
                await DelayAsync(options.Advanced.ControlPollInterval, ct);
                continue;
            }

            await subscription.WaitAsync(options.Advanced.ControlPollInterval, ct);
        }
    }

    private static Task DelayAsync(TimeSpan delay, CancellationToken ct)
        // A non-positive value (reachable via post-validation configuration) would throw or spin; floor it.
        => Task.Delay(delay <= TimeSpan.Zero ? TimeSpan.FromSeconds(1) : delay, ct);
}

/// <summary>Source-generated log messages for <see cref="ControlGate"/>.</summary>
internal static partial class ControlGateLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Finalizing Wallaby suspension for slot {Slot}: dropping every managed replication slot.")]
    internal static partial void FinalizingSuspension(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Wallaby is suspended (slot {Slot}): managed replication slots are dropped and streaming is stopped until an explicit resume.")]
    internal static partial void Suspended(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Wallaby is suspended (slot {Slot}) by nodes still deployed with Suspend(); this flag-less node is waiting out the configuration-suspension grace and will auto-resume once their assertion goes stale.")]
    internal static partial void SuspendedAwaitingGrace(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby suspension ended (slot {Slot}); managed replication slots are recreated on the next bootstrap, and a capturing node re-backfills all mapped tables.")]
    internal static partial void SuspensionEnded(this ILogger logger, string slot);
}
