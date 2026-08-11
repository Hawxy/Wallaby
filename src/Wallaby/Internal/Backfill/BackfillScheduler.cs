using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
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
/// </summary>
internal sealed class BackfillScheduler(
    IReadOnlyList<BackfillTable> tables,
    IBackfillStateStore store,
    WatermarkBackfillCoordinator coordinator,
    SinkPurgeRunner purger,
    BackfillSchedulerOptions options,
    ILogger logger)
{
    /// <summary>
    /// Run for the lifetime of leadership: an initial due pass, then serve manual backfill requests
    /// as they arrive. Woken by the backfill notify channel; <paramref name="pollInterval"/> is only
    /// the safety net for a missed notification. Requests are checked before each wait, so one
    /// arriving during a pass is never stranded until the poll.
    /// </summary>
    public async Task RunAsync(TimeSpan pollInterval, CancellationToken ct)
    {
        await using var signal = store.Subscribe();
        var mappedNames = tables.Select(t => t.Table.QualifiedName).ToHashSet(StringComparer.Ordinal);
        var warnedUnknown = new HashSet<string>(StringComparer.Ordinal);

        await RunDueBackfillsAsync(ct);
        while (!ct.IsCancellationRequested)
        {
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
            if (due > 0)
            {
                logger.BackfillRequestsObserved(due);
                await RunDueBackfillsAsync(ct);
            }
            else
            {
                await signal.WaitAsync(pollInterval, ct);
            }
        }
    }

    public async Task RunDueBackfillsAsync(CancellationToken ct)
    {
        foreach (var table in tables)
        {
            var qualifiedName = table.Table.QualifiedName;
            var state = await store.GetAsync(qualifiedName, ct);
            var decision = Decide(state, table.TransformVersion, table.PurgeOnVersionChange, options);
            if (decision.Action == BackfillAction.Skip)
            {
                continue;
            }

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
        }
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
}
