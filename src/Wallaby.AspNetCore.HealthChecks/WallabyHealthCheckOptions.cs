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

    /// <summary>
    /// Consecutive failures of a dependent fan-out job before the check reports Degraded. Live replication
    /// is unaffected by a stuck fan-out job — only the dependent re-syncs it would have driven — so this
    /// grades Degraded rather than Unhealthy, keeping a liveness probe from restart-looping the node while
    /// still surfacing that some documents are going stale. The counter tracks the worst pending job's
    /// persisted failure streak, so it holds while that job is backed off (even as other jobs drain) and
    /// clears when the job finally completes. Set to 0 (or negative) to disable fan-out grading.
    /// Defaults to 5.
    /// </summary>
    public int FanoutFailureThreshold { get; set; } = 5;
}
