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
    /// Consecutive fan-out failures before the check reports Degraded: the worse of the worst pending
    /// job's persisted failure streak (holds while that job is backed off, even as other jobs drain;
    /// clears when it finally completes) and the drain loop's consecutive pass failures (the queue
    /// itself unreachable). Live replication is unaffected by either — only the dependent re-syncs they
    /// would have driven — so this grades Degraded rather than Unhealthy, keeping a liveness probe from
    /// restart-looping the node while still surfacing that some documents are going stale. Set to 0
    /// (or negative) to disable fan-out grading. Defaults to 5.
    /// </summary>
    public int FanoutFailureThreshold { get; set; } = 5;

    /// <summary>
    /// Consecutive backfill failures before the check reports Degraded: the worse of the worst pending
    /// table's persisted failure streak (holds while that table is backed off; clears when its run
    /// finally starts fresh or completes) and the scheduler's consecutive pass failures (the state
    /// store itself unreachable). Live replication and the other tables' backfills are unaffected —
    /// the failing table retries with backoff — so this grades Degraded rather than Unhealthy, keeping
    /// a liveness probe from restart-looping the node while still surfacing that the table's sinks are
    /// not converging. Set to 0 (or negative) to disable backfill grading. Defaults to 5.
    /// </summary>
    public int BackfillFailureThreshold { get; set; } = 5;
}
