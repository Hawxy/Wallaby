using Microsoft.Extensions.Diagnostics.HealthChecks;
using Wallaby.Abstractions;

namespace Wallaby.AspNetCore.HealthChecks;

/// <summary>
/// Wallaby liveness health check
/// </summary>
public sealed class WallabyHealthCheck(ICdcStatus status) : IHealthCheck
{
    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var snapshot = status.Current;
        var data = Describe(snapshot);

        var result = snapshot.Faulted
            ? HealthCheckResult.Unhealthy("CDC background service terminated.", exception: null, data)
            : HealthCheckResult.Healthy("CDC subsystem alive.", data);

        return Task.FromResult(result);
    }

    private static Dictionary<string, object> Describe(CdcStatusSnapshot s)
    {
        var data = new Dictionary<string, object>
        {
            ["role"] = s.Role.ToString(),
            ["faulted"] = s.Faulted,
            ["startedAt"] = s.StartedAt,
            ["lastAcknowledgedLsn"] = s.LastAcknowledgedLsn,
            ["consecutiveLeaderFailures"] = s.ConsecutiveLeaderFailures,
            ["slotName"] = s.SlotName,
        };
        if (s.LastError is { } error) data["lastError"] = error;
        if (s.LeaderSince is { } leaderSince) data["leaderSince"] = leaderSince;
        if (s.LastProgressAt is { } progress) data["lastProgressAt"] = progress;
        if (s.LastIngestionLagSeconds >= 0) data["lastIngestionLagSeconds"] = s.LastIngestionLagSeconds;
        return data;
    }
}
