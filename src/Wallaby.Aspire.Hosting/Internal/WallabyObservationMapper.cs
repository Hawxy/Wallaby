using System.Collections.Immutable;
using System.Globalization;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Wallaby.Client;

namespace Wallaby.Aspire.Hosting.Internal;

/// <summary>Turns a poll into what the dashboard shows: properties, health and command availability.</summary>
internal static class WallabyObservationMapper
{
    private const string PropertyPrefix = "wallaby.";

    public static HealthCheckResult Health(WallabyObservation? observation)
    {
        var health = Grade(observation);
        return health.Status == HealthStatus.Healthy && observation?.PreviouslyCaughtUp != true
            ? HealthCheckResult.Degraded("Caught up; confirming on the next poll.")
            : health;
    }

    public static bool IsCaughtUp(WallabyObservation observation) => Grade(observation).Status == HealthStatus.Healthy;

    private static HealthCheckResult Grade(WallabyObservation? observation)
    {
        if (observation is null)
        {
            return HealthCheckResult.Degraded("Connecting to the database.");
        }
        if (observation.Error is { } error)
        {
            return HealthCheckResult.Unhealthy($"Polling the Wallaby control plane failed: {error}");
        }

        var control = observation.Control;
        if (control.State == WallabySuspensionState.Unknown
            || control.Slots.Any(s => s.Kind == WallabyManagedSlotKind.Unknown)
            || observation.Backfills.Any(b => b.Status == WallabyBackfillStatus.Unknown))
        {
            return HealthCheckResult.Unhealthy(
                "The database holds Wallaby state written by a newer host than this Wallaby.Aspire.Hosting version understands; upgrade the package.");
        }
        if (control.Slots.FirstOrDefault(s => s.InvalidationReason is not null) is { } invalidated)
        {
            return HealthCheckResult.Unhealthy(
                $"Replication slot {invalidated.SlotName} was invalidated ({invalidated.InvalidationReason}); the next Wallaby leader recreates it and re-backfills.");
        }
        if (control.State is WallabySuspensionState.SuspendRequested or WallabySuspensionState.Suspended)
        {
            return HealthCheckResult.Degraded($"Wallaby is {Describe(control.State)}; replication is stopped until a resume.");
        }
        if (control.Slots.Count == 0)
        {
            return HealthCheckResult.Degraded("Waiting for a Wallaby host to provision its replication slots.");
        }
        if (control.Slots.FirstOrDefault(s => !s.ExistsOnServer) is { } missing)
        {
            return HealthCheckResult.Degraded($"Replication slot {missing.SlotName} does not exist on the server yet.");
        }
        // External slots are read by third-party consumers that may legitimately be offline in development.
        if (control.Slots.FirstOrDefault(s => s.Kind == WallabyManagedSlotKind.Primary && !s.Active) is { } idle)
        {
            return HealthCheckResult.Degraded($"Replication slot {idle.SlotName} has no active consumer; is the Wallaby host running?");
        }
        if (observation.Backfills.FirstOrDefault(b => IsPending(b) && b.Attempts > 0) is { } failing)
        {
            return HealthCheckResult.Degraded($"Backfill of {failing.Table} is failing: {DescribeBackfill(failing)}");
        }
        if (observation.Backfills.Where(IsPending).ToList() is { Count: > 0 } pending)
        {
            return HealthCheckResult.Degraded(
                "Backfilling " + string.Join(", ", pending.Select(b => $"{b.Table} ({DescribeBackfill(b)})")) + ".");
        }
        // Widening is a deliberate, temporary operator action while replication keeps streaming.
        return HealthCheckResult.Healthy(control.PublicationsWidened
            ? "Streaming and caught up; publications are widened for a schema migration."
            : "Streaming and caught up.");
    }

    public static ImmutableArray<ResourcePropertySnapshot> Properties(WallabyObservation observation)
    {
        var control = observation.Control;
        var properties = ImmutableArray.CreateBuilder<ResourcePropertySnapshot>();
        // Unrecognized properties are hidden by the dashboard unless highlighted; the sort order groups
        // the poll error, suspension and publications first, then slots, then backfills.
        var order = 0;
        void Add(string key, string displayName, string value)
            => properties.Add(new ResourcePropertySnapshot(key, value)
            {
                DisplayName = displayName,
                IsHighlighted = true,
                SortOrder = 1000 + order++,
            });

        if (observation.Error is { } error)
        {
            Add($"{PropertyPrefix}error", "Poll error", error);
        }
        Add($"{PropertyPrefix}suspension", "Suspension", DescribeSuspension(control));
        if (control.PublicationsWidened)
        {
            Add($"{PropertyPrefix}publications", "Publications", $"Widened{By(control.WidenedBy)}{Since(control.WidenedAt)}");
        }
        foreach (var slot in control.Slots)
        {
            Add($"{PropertyPrefix}slot.{slot.SlotName}", $"Slot {slot.SlotName}", DescribeSlot(slot));
        }
        foreach (var backfill in observation.Backfills)
        {
            Add($"{PropertyPrefix}backfill.{backfill.Table}", $"Backfill {backfill.Table}", DescribeBackfill(backfill));
        }
        return properties.ToImmutable();
    }

    /// <summary>
    /// Replace the previous poll's properties while keeping the ones Aspire stamps on the snapshot (such
    /// as the parent name the dashboard nests by).
    /// </summary>
    public static ImmutableArray<ResourcePropertySnapshot> MergeProperties(
        ImmutableArray<ResourcePropertySnapshot> existing, ImmutableArray<ResourcePropertySnapshot> wallaby)
        => [.. existing.Where(p => !p.Name.StartsWith(PropertyPrefix, StringComparison.Ordinal)), .. wallaby];

    /// <summary>
    /// Replace the report named <paramref name="name"/> with <paramref name="health"/>, keeping its run
    /// time when the verdict is unchanged so an unchanged poll publishes nothing.
    /// </summary>
    public static ImmutableArray<HealthReportSnapshot> MergeHealthReport(
        ImmutableArray<HealthReportSnapshot> existing, string name, HealthCheckResult health)
    {
        var previous = existing.FirstOrDefault(r => r.Name == name);
        if (previous is not null && previous.Status == health.Status && previous.Description == health.Description)
        {
            return existing;
        }
        var report = new HealthReportSnapshot(name, health.Status, health.Description, health.Exception?.ToString())
        {
            LastRunAt = DateTime.UtcNow,
        };
        return [.. existing.Where(r => r.Name != name), report];
    }

    public static bool CanBackfill(WallabyMonitorState monitor)
        => monitor.Client is not null && monitor.Latest?.Control.State == WallabySuspensionState.Running;

    public static bool CanBackfillAll(WallabyMonitorState monitor)
        => CanBackfill(monitor) && monitor.Latest!.Backfills.Count > 0;

    public static bool CanCancelBackfill(WallabyMonitorState monitor)
        => monitor.Client is not null && monitor.Latest?.Backfills.Any(b => b.Status == WallabyBackfillStatus.Requested) == true;

    public static bool CanSuspend(WallabyMonitorState monitor) => CanBackfill(monitor);

    public static bool CanResume(WallabyMonitorState monitor)
        => monitor.Client is not null
           && monitor.Latest?.Control.State is WallabySuspensionState.SuspendRequested or WallabySuspensionState.Suspended;

    public static bool CanWiden(WallabyMonitorState monitor)
        => CanBackfill(monitor) && !monitor.Latest!.Control.PublicationsWidened;

    public static bool CanRestore(WallabyMonitorState monitor)
        => monitor.Client is not null && monitor.Latest?.Control.PublicationsWidened == true;

    public static string Describe(WallabySuspensionState state) => state switch
    {
        WallabySuspensionState.Running => "running",
        WallabySuspensionState.SuspendRequested => "suspending",
        WallabySuspensionState.Suspended => "suspended",
        _ => "in an unknown state",
    };

    public static string DescribeSlot(WallabyManagedSlot slot)
    {
        var kind = slot.Kind == WallabyManagedSlotKind.External ? "external, " : string.Empty;
        if (slot.InvalidationReason is { } reason)
        {
            return $"{kind}invalidated ({reason})";
        }
        if (!slot.ExistsOnServer)
        {
            return $"{kind}missing on the server";
        }
        var activity = slot.Active ? "streaming" : "no consumer";
        var retained = slot.RetainedWalBytes is { } bytes ? $", {FormatBytes(bytes)} WAL retained" : string.Empty;
        return $"{kind}{activity}{retained}";
    }

    public static string DescribeBackfill(WallabyBackfillState backfill)
    {
        var text = backfill.Status switch
        {
            WallabyBackfillStatus.InProgress when backfill.Progress is { } progress =>
                $"in progress, {(progress * 100).ToString("0", CultureInfo.InvariantCulture)}%"
                + (backfill.EstimatedRemaining is { } left ? $" (~{FormatDuration(left)} left)" : string.Empty),
            WallabyBackfillStatus.InProgress => $"in progress, {backfill.RowsCopied.ToString("N0", CultureInfo.InvariantCulture)} rows",
            WallabyBackfillStatus.Requested => "requested",
            WallabyBackfillStatus.Completed => $"completed, {backfill.RowsCopied.ToString("N0", CultureInfo.InvariantCulture)} rows",
            WallabyBackfillStatus.Cancelled => "cancelled",
            _ => "unknown status",
        };
        if (IsPending(backfill) && backfill.Attempts > 0)
        {
            text += $"; attempt {backfill.Attempts} failed"
                    + (backfill.LastError is { } error ? $": {error}" : string.Empty)
                    + (backfill.NextAttemptAt is { } next ? $" (retrying at {next.ToLocalTime():T})" : string.Empty);
        }
        return text;
    }

    private static string DescribeSuspension(WallabyControlState control)
    {
        if (control.State == WallabySuspensionState.Running)
        {
            return "Running";
        }
        var origin = control.Origin == WallabySuspensionOrigin.Configuration ? "by configuration" : "by a client";
        var reason = control.Reason is { } r ? $": {r}" : string.Empty;
        var state = control.State == WallabySuspensionState.Suspended ? "Suspended" : "Suspend requested";
        return $"{state} {origin}{By(control.RequestedBy)}{reason}";
    }

    private static bool IsPending(WallabyBackfillState b)
        => b.Status is WallabyBackfillStatus.Requested or WallabyBackfillStatus.InProgress;

    private static string By(string? who) => who is null ? string.Empty : $" ({who})";

    private static string Since(DateTimeOffset? at) => at is { } t ? $" since {t.ToLocalTime():g}" : string.Empty;

    internal static string FormatBytes(long bytes)
    {
        string[] units = ["B", "KB", "MB", "GB", "TB"];
        double value = bytes;
        var unit = 0;
        while (value >= 1024 && unit < units.Length - 1)
        {
            value /= 1024;
            unit++;
        }
        return unit == 0
            ? $"{bytes} B"
            : $"{value.ToString("0.#", CultureInfo.InvariantCulture)} {units[unit]}";
    }

    private static string FormatDuration(TimeSpan duration) => duration switch
    {
        { TotalHours: >= 1 } => $"{(int)duration.TotalHours}h {duration.Minutes}m",
        { TotalMinutes: >= 1 } => $"{(int)duration.TotalMinutes}m {duration.Seconds}s",
        _ => $"{Math.Max(1, (int)duration.TotalSeconds)}s",
    };
}
