using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.State;

namespace Wallaby.Internal.Backfill;

/// <summary>Options governing automatic backfills.</summary>
internal sealed class BackfillSchedulerOptions
{
    /// <summary>Backfill a newly declared table that has no recorded state.</summary>
    public bool AutoBackfillNewTables { get; init; } = true;

    /// <summary>Re-backfill a completed table when its declared transform version changed.</summary>
    public bool AutoBackfillOnVersionChange { get; init; } = true;
}

/// <summary>What the scheduler should do for a table.</summary>
internal enum BackfillAction
{
    Skip,
    Fresh,  // start from the beginning (new table, requested, or version changed)
    Resume, // continue an interrupted backfill from its cursor
}

/// <summary>The scheduler's decision for a table: the action, and whether a sink purge precedes it.</summary>
internal readonly record struct BackfillDecision(BackfillAction Action, bool Purge);

/// <summary>
/// Decides which tables need backfilling at startup (and on demand) and runs them via the coordinator.
/// Automatic triggers: a new table, a changed transform version, an interrupted (in-progress) backfill;
/// manual triggers: a state row marked <see cref="BackfillStatus.Requested"/> by the backfill manager.
/// Intended to run on the leader.
/// <para>Each table is isolated, mirroring the fan-out queue: a failure backs off that table alone
/// (persisted attempts/backoff in <c>wallaby.backfill_state</c>) and the other tables keep running.</para>
/// </summary>
internal sealed class BackfillScheduler(
    IReadOnlyList<BackfillTable> tables,
    IBackfillStateStore store,
    WatermarkBackfillCoordinator coordinator,
    SinkPurgeRunner purger,
    BackfillSchedulerOptions options,
    ILogger logger,
    WallabyStatus? status = null)
{
    // Base delay before retrying after a failed scheduler pass (the store itself unreachable;
    // individual table failures are handled per table), growing exponentially to a cap.
    private static readonly TimeSpan BaseErrorRetryDelay = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Run for the lifetime of leadership: an initial due pass, then serve manual backfill requests
    /// as they arrive. Woken by the backfill notify channel; <paramref name="pollInterval"/> is only
    /// the safety net for a missed notification, and the wait shortens to the soonest backed-off
    /// table's retry time. Requests are checked before each wait, so one arriving during a pass is
    /// never stranded until the poll. Never faults the leader: a failing pass retries with backoff.
    /// </summary>
    public async Task RunAsync(TimeSpan pollInterval, CancellationToken ct)
    {
        await using var signal = store.Subscribe();
        var mappedNames = tables.Select(t => t.Table.QualifiedName).ToHashSet(StringComparer.Ordinal);
        var warnedUnknown = new HashSet<string>(StringComparer.Ordinal);
        var errorBackoff = new RetryBackoff(BaseErrorRetryDelay);

        var passDue = true; // the initial due pass
        DateTimeOffset? nextRetryAt = null;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (passDue)
                {
                    nextRetryAt = await RunDueBackfillsAsync(ct);
                    if (status is not null)
                    {
                        // The failure counter mirrors the worst pending table's persisted attempts, so a
                        // clean pass can't mask a failing table that is merely backed off right now.
                        status.SetBackfillStreak(await store.MaxAttemptsAsync(ct));
                    }
                    errorBackoff.Reset();
                }

                var requested = await store.ListRequestedAsync(ct);

                // A request naming a table no mapping captures sits queued until a mapping for it deploys.
                // That is legal (requests may precede a deploy), but a typo would wait forever silently, so
                // each unknown name is called out once per leadership term.
                foreach (var name in requested)
                {
                    if (!mappedNames.Contains(name) && warnedUnknown.Add(name))
                    {
                        logger.UnknownBackfillRequested(name);
                    }
                }

                var due = requested.Count(mappedNames.Contains);
                passDue = due > 0 || (nextRetryAt is { } retryAt && retryAt <= DateTimeOffset.UtcNow);
                if (due > 0)
                {
                    logger.BackfillRequestsObserved(due);
                }
                else if (!passDue)
                {
                    var wait = pollInterval;
                    if (nextRetryAt is { } retry)
                    {
                        var until = retry - DateTimeOffset.UtcNow;
                        if (until < wait)
                        {
                            wait = until > TimeSpan.Zero ? until : TimeSpan.Zero;
                        }
                    }
                    await signal.WaitAsync(wait, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.SchedulerPassFailed(ex);
                status?.RecordBackfillFailure($"{ex.GetType().Name}: {ex.Message}");
                try { await Task.Delay(errorBackoff.Next(), ct); }
                catch (OperationCanceledException) { break; }
                passDue = true;
            }
        }
    }

    /// <summary>
    /// Run every table that is due right now. Returns the soonest time a backed-off (failing) table
    /// becomes due again, or null when none is pending a retry.
    /// </summary>
    public async Task<DateTimeOffset?> RunDueBackfillsAsync(CancellationToken ct)
    {
        DateTimeOffset? nextRetryAt = null;
        void TrackRetry(DateTimeOffset at)
            => nextRetryAt = nextRetryAt is { } current && current <= at ? current : at;

        foreach (var table in tables)
        {
            var qualifiedName = table.Table.QualifiedName;
            var state = await store.GetAsync(qualifiedName, ct);
            var decision = Decide(state, table.TransformVersion, table.PurgeOnVersionChange, options);
            if (decision.Action == BackfillAction.Skip)
            {
                continue;
            }
            if (state?.NextAttemptAt is { } notBefore && notBefore > DateTimeOffset.UtcNow)
            {
                // In failure backoff; due again when it expires (a manual request during the window
                // is served then, not immediately).
                TrackRetry(notBefore);
                continue;
            }

            try
            {
                if (decision.Action == BackfillAction.Fresh)
                {
                    if (decision.Purge)
                    {
                        // Purging before the snapshot read converges: the snapshot re-upserts every row that
                        // exists at read time, and live changes win over snapshot rows within each chunk
                        // window. Runs before the InProgress save so a crash in between re-detects the
                        // request and re-purges (idempotent); the save clears the durable flag, so a
                        // resumed backfill never purges away its own delivered chunks.
                        await purger.PurgeAsync(table, ct);
                    }

                    // Reset the cursor so the coordinator starts from the beginning.
                    await store.SaveAsync(
                        new BackfillState(
                            qualifiedName, BackfillStatus.InProgress, table.TransformVersion, null, 0,
                            DateTimeOffset.UtcNow),
                        ct);
                }

                logger.BackfillScheduled(decision.Action, qualifiedName, table.TransformVersion);
                await coordinator.BackfillTableAsync(table.Table, ct);
                await store.ClearFailureAsync(qualifiedName, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // Left to escape, one failing table would fault the whole leader session and stall CDC
                // for every table; instead it backs off alone and the pass moves on.
                var error = $"{ex.GetType().Name}: {ex.Message}";
                var attempt = (state?.Attempts ?? 0) + 1;
                logger.BackfillTableFailed(qualifiedName, attempt, ex);
                status?.RecordBackfillTableFailure(error, attempt);
                TrackRetry(await store.FailAsync(qualifiedName, error, ct));
            }
        }
        return nextRetryAt;
    }

    /// <summary>Pure decision used by the scheduler (separated for testability).</summary>
    public static BackfillDecision Decide(
        BackfillState? state, string? declaredVersion, bool purgeOnVersionChange, BackfillSchedulerOptions options)
    {
        if (state is null)
        {
            return new(options.AutoBackfillNewTables ? BackfillAction.Fresh : BackfillAction.Skip, Purge: false);
        }

        return state.Status switch
        {
            BackfillStatus.Requested or BackfillStatus.NotStarted => new(BackfillAction.Fresh, state.Purge),
            // Re-purging mid-run would delete the chunks the run already delivered.
            BackfillStatus.InProgress => new(BackfillAction.Resume, Purge: false),
            // A cancelled table stays skipped (even on a version change) until a new request marks it
            // Requested again.
            BackfillStatus.Cancelled => new(BackfillAction.Skip, Purge: false),
            // A version-change Fresh fires from a Completed row (its flag is false — completion clears
            // it), so the purge intent comes from the mappings' opt-in.
            BackfillStatus.Completed when options.AutoBackfillOnVersionChange && state.TransformVersion != declaredVersion
                => new(BackfillAction.Fresh, state.Purge || purgeOnVersionChange),
            _ => new(BackfillAction.Skip, Purge: false),
        };
    }
}

/// <summary>Source-generated log messages for <see cref="BackfillScheduler"/>.</summary>
internal static partial class BackfillSchedulerLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Backfill {Action} for {Table} (version {Version}).")]
    internal static partial void BackfillScheduled(this ILogger logger, BackfillAction action, string table, string? version);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Count} manual backfill request(s) observed; running a scheduler pass.")]
    internal static partial void BackfillRequestsObserved(this ILogger logger, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "A backfill of {Table} is requested, but no mapping captures that table. The request stays queued until a mapping for it deploys; if the name is wrong, cancel it (IWallabyBackfillManager or WallabyControlClient, CancelBackfillAsync).")]
    internal static partial void UnknownBackfillRequested(this ILogger logger, string table);

    [LoggerMessage(Level = LogLevel.Error, Message = "Backfill of {Table} failed (attempt {Attempt}); it will retry with backoff while other tables continue.")]
    internal static partial void BackfillTableFailed(this ILogger logger, string table, int attempt, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Backfill scheduler pass failed; retrying.")]
    internal static partial void SchedulerPassFailed(this ILogger logger, Exception ex);
}
