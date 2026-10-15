using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wallaby.Client;

namespace Wallaby.Aspire.Hosting.Internal;

/// <summary>
/// The poll loop behind a <see cref="WallabyResource"/>: waits for the database, then reads the control
/// plane every <see cref="WallabyResource.PollInterval"/> and publishes what changed to the dashboard.
/// </summary>
internal static class WallabyMonitor
{
    public static Task StartAsync(WallabyResource resource, InitializeResourceEvent evt, CancellationToken ct)
    {
        var stopping = evt.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping;
        _ = Task.Run(() => RunAsync(resource, evt.Notifications, evt.Logger, stopping), CancellationToken.None);
        return Task.CompletedTask;
    }

    private static async Task RunAsync(
        WallabyResource resource, ResourceNotificationService notifications, ILogger logger, CancellationToken ct)
    {
        try
        {
            await notifications.PublishUpdateAsync(resource, s => s with
            {
                ResourceType = "Wallaby",
                State = KnownResourceStates.Waiting,
                CreationTimeStamp = s.CreationTimeStamp ?? DateTime.UtcNow,
            });
            await notifications.WaitForDependenciesAsync(resource, ct);

            var connectionString = await resource.Database.ConnectionStringExpression.GetValueAsync(ct);
            if (string.IsNullOrEmpty(connectionString))
            {
                logger.ConnectionStringMissing(resource.Database.Name);
                await notifications.PublishUpdateAsync(resource, s => s with { State = KnownResourceStates.FailedToStart });
                return;
            }

            await using var client = new WallabyControlClient(connectionString);
            resource.Monitor.Client = client;
            await notifications.PublishUpdateAsync(resource, s => s with
            {
                State = KnownResourceStates.Running,
                StartTimeStamp = DateTime.UtcNow,
            });

            WallabyObservation? previous = null;
            while (!ct.IsCancellationRequested)
            {
                var current = await PollAsync(client, previous, ct);
                resource.Monitor.Latest = current;
                WallabyChangeLog.Write(logger, previous, current);
                // Publishing an unchanged snapshot is a no-op, so this only reaches the dashboard on change.
                // Aspire re-runs a healthy resource's checks only every 30 seconds, so the poll also writes
                // the same verdict into the snapshot's health report to show changes right away.
                var properties = WallabyObservationMapper.Properties(current);
                var health = WallabyObservationMapper.Health(current);
                await notifications.PublishUpdateAsync(resource, s => (s with
                {
                    Properties = WallabyObservationMapper.MergeProperties(s.Properties, properties),
                }).WithHealthReports(WallabyObservationMapper.MergeHealthReport(s.HealthReports, resource.HealthCheckKey, health)));
                previous = current;
                await resource.Monitor.WaitForNextPollAsync(resource.PollInterval, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            // E.g. a dependency that failed to start; the loop runs detached, so surface it here.
            logger.MonitorFailed(ex);
            await notifications.PublishUpdateAsync(resource, s => s with { State = KnownResourceStates.FailedToStart });
        }
        finally
        {
            resource.Monitor.Client = null;
        }
    }

    internal static async Task<WallabyObservation> PollAsync(
        WallabyControlClient client, WallabyObservation? previous, CancellationToken ct)
    {
        try
        {
            var control = await client.GetStateAsync(ct);
            var backfills = await client.GetBackfillStatusAsync(ct);
            return new WallabyObservation(control, backfills,
                PreviouslyCaughtUp: previous is not null && WallabyObservationMapper.IsCaughtUp(previous));
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            var lastGood = previous ?? new WallabyObservation(
                new WallabyControlState(WallabySuspensionState.Running, WallabySuspensionOrigin.Client,
                    null, null, null, null, null, []),
                []);
            return lastGood with { Error = ex.Message, PreviouslyCaughtUp = false };
        }
    }
}

/// <summary>Writes state transitions to the resource's console log in the dashboard.</summary>
internal static class WallabyChangeLog
{
    public static void Write(ILogger logger, WallabyObservation? previous, WallabyObservation current)
    {
        if (current.Error != previous?.Error)
        {
            if (current.Error is { } error)
            {
                logger.PollFailed(error);
            }
            else
            {
                logger.PollRecovered();
            }
        }
        if (current.Error is not null)
        {
            return;
        }

        if (previous is null || previous.Control.State != current.Control.State)
        {
            logger.SuspensionChanged(WallabyObservationMapper.Describe(current.Control.State));
        }
        if (previous is not null && previous.Control.PublicationsWidened != current.Control.PublicationsWidened)
        {
            logger.WideningChanged(current.Control.PublicationsWidened ? "widened" : "restored");
        }

        var previousSlots = previous?.Control.Slots.ToDictionary(s => s.SlotName) ?? [];
        foreach (var slot in current.Control.Slots)
        {
            previousSlots.TryGetValue(slot.SlotName, out var before);
            if (slot.InvalidationReason is { } reason && before?.InvalidationReason is null)
            {
                logger.SlotInvalidated(slot.SlotName, reason);
            }
            else if (before is null || before.Active != slot.Active || before.ExistsOnServer != slot.ExistsOnServer)
            {
                logger.SlotChanged(slot.SlotName, WallabyObservationMapper.DescribeSlot(slot));
            }
        }

        var previousBackfills = previous?.Backfills.ToDictionary(b => b.Table) ?? [];
        foreach (var backfill in current.Backfills)
        {
            previousBackfills.TryGetValue(backfill.Table, out var before);
            if (before is null || before.Status != backfill.Status)
            {
                logger.BackfillChanged(backfill.Table, WallabyObservationMapper.DescribeBackfill(backfill));
            }
            else if (backfill.Attempts > before.Attempts)
            {
                logger.BackfillFailed(backfill.Table, WallabyObservationMapper.DescribeBackfill(backfill));
            }
        }
    }
}

internal static partial class WallabyMonitorLog
{
    [LoggerMessage(Level = LogLevel.Error, Message = "Database resource {Database} has no connection string; the Wallaby monitor cannot start.")]
    public static partial void ConnectionStringMissing(this ILogger logger, string database);

    [LoggerMessage(Level = LogLevel.Error, Message = "The Wallaby monitor stopped.")]
    public static partial void MonitorFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Polling the Wallaby control plane failed: {Error}")]
    public static partial void PollFailed(this ILogger logger, string error);

    [LoggerMessage(Level = LogLevel.Information, Message = "Polling the Wallaby control plane recovered.")]
    public static partial void PollRecovered(this ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby is {State}.")]
    public static partial void SuspensionChanged(this ILogger logger, string state);

    [LoggerMessage(Level = LogLevel.Information, Message = "Publications {Change}.")]
    public static partial void WideningChanged(this ILogger logger, string change);

    [LoggerMessage(Level = LogLevel.Information, Message = "Slot {Slot}: {Description}.")]
    public static partial void SlotChanged(this ILogger logger, string slot, string description);

    [LoggerMessage(Level = LogLevel.Error, Message = "Slot {Slot} was invalidated ({Reason}); the next Wallaby leader recreates it and re-backfills.")]
    public static partial void SlotInvalidated(this ILogger logger, string slot, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "Backfill {Table}: {Description}.")]
    public static partial void BackfillChanged(this ILogger logger, string table, string description);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Backfill {Table}: {Description}.")]
    public static partial void BackfillFailed(this ILogger logger, string table, string description);
}
