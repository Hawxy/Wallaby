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
        if (s.LastProgressAt is { } progress) data["lastProgressAt"] = progress;
        if (s.LastIngestionLagSeconds >= 0) data["lastIngestionLagSeconds"] = s.LastIngestionLagSeconds;
        foreach (var (sink, at) in s.LastSinkDeliveryAt)
        {
            data[$"lastSinkDeliveryAt:{sink}"] = at;
        }
        return data;
    }
}
