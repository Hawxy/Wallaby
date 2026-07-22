namespace Wallaby.AspNetCore.HealthChecks;

/// <summary>Options for <see cref="WallabyHealthCheck"/>.</summary>
public sealed class WallabyHealthCheckOptions
{
    /// <summary>
    /// Consecutive leader-session failures before the check reports Unhealthy. A leader that fails
    /// repeatedly without making progress is crash-looping (e.g. a poison event or a sink permanently
    /// rejecting a batch) and delivery is stalled until the cause is resolved; successful progress
    /// resets the counter. Set to 0 (or negative) to disable crash-loop grading. Defaults to 3.
    /// </summary>
    public int CrashLoopFailureThreshold { get; set; } = 3;
}
