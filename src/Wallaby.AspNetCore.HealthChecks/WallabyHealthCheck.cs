using Microsoft.Extensions.Diagnostics.HealthChecks;
using Wallaby.Abstractions;

namespace Wallaby.AspNetCore.HealthChecks;

/// <summary>
/// Wallaby liveness health check
/// </summary>
public sealed class WallabyHealthCheck(IWallabyStatus status, WallabyHealthCheckOptions? options = null) : IHealthCheck
{
    private readonly WallabyHealthCheckOptions _options = options ?? new();

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var snapshot = status.Current;
        var data = Describe(snapshot);
        var crashLoopThreshold = _options.CrashLoopFailureThreshold;
        var fanoutThreshold = _options.FanoutFailureThreshold;

        var result = snapshot switch
        {
            { Faulted: true } => HealthCheckResult.Unhealthy(
                "Wallaby background service terminated.", exception: null, data),
            // A leader that keeps failing without acknowledging progress is crash-looping (poison event,
            // permanently rejecting sink, ...): delivery is stalled even though the process is alive.
            { ConsecutiveLeaderFailures: var failures } when crashLoopThreshold > 0 && failures >= crashLoopThreshold =>
                HealthCheckResult.Unhealthy(
                    $"Wallaby leader is crash-looping ({failures} consecutive failures)." +
                    (snapshot.LastError is { } lastError ? $" Last error: {lastError}" : string.Empty),
                    exception: null, data),
            // Deliberate but loud: the node is alive (don't restart-loop it) while replication is
            // deliberately stopped and its slots are dropped (e.g. for a database major-version upgrade).
            { Role: WallabyNodeRole.Suspended } => HealthCheckResult.Degraded(
                "Wallaby is suspended: managed replication slots are dropped until an explicit resume.", exception: null, data),
            // Dependent documents go stale, but live replication is fine: loud, not a restart signal.
            { ConsecutiveFanoutFailures: var fanoutFailures } when fanoutThreshold > 0 && fanoutFailures >= fanoutThreshold =>
                HealthCheckResult.Degraded(
                    $"Wallaby dependent fan-out is failing ({fanoutFailures} consecutive job failures); " +
                    "documents that depend on those tables are going stale." +
                    (snapshot.LastError is { } fanoutError ? $" Last error: {fanoutError}" : string.Empty),
                    exception: null, data),
            _ => HealthCheckResult.Healthy("Wallaby subsystem alive.", data),
        };

        return Task.FromResult(result);
    }

    private static Dictionary<string, object> Describe(WallabyStatusSnapshot s)
    {
        var data = new Dictionary<string, object>
        {
            ["role"] = s.Role.ToString(),
            ["faulted"] = s.Faulted,
            ["startedAt"] = s.StartedAt,
            ["lastAcknowledgedLsn"] = s.LastAcknowledgedLsn,
            ["consecutiveLeaderFailures"] = s.ConsecutiveLeaderFailures,
            ["consecutiveFanoutFailures"] = s.ConsecutiveFanoutFailures,
            ["slotName"] = s.SlotName,
        };
        if (s.LastError is { } error) data["lastError"] = error;
        if (s.LeaderSince is { } leaderSince) data["leaderSince"] = leaderSince;
        if (s.SuspendedSince is { } suspendedSince) data["suspendedSince"] = suspendedSince;
        if (s.SuspensionReason is { } suspensionReason) data["suspensionReason"] = suspensionReason;
        // Deliberately Healthy while widened: capture is fully functional, only the column-list
        // narrowing is temporarily lifted.
        if (s.PublicationsWidened)
        {
            data["publicationsWidened"] = true;
            if (s.PublicationsWidenedAt is { } widenedAt) data["publicationsWidenedAt"] = widenedAt;
        }
        if (s.LastProgressAt is { } progress) data["lastProgressAt"] = progress;
        if (s.LastIngestionLagSeconds >= 0) data["lastIngestionLagSeconds"] = s.LastIngestionLagSeconds;
        foreach (var (sink, at) in s.LastSinkDeliveryAt)
        {
            data[$"lastSinkDeliveryAt:{sink}"] = at;
        }
        return data;
    }
}
