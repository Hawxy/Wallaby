using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Internal.State;
using Wallaby.Model;

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

/// <summary>
/// Decides which tables need backfilling at startup (and on demand) and runs them via the coordinator.
/// Automatic triggers: a new table, a changed transform version, an interrupted (in-progress) backfill;
/// manual triggers: a state row marked <see cref="BackfillStatus.Requested"/> by the backfill manager.
/// Intended to run on the leader.
/// </summary>
internal sealed class BackfillScheduler(
    IReadOnlyList<(CapturedTable Table, string? TransformVersion)> tables,
    IBackfillStateStore store,
    WatermarkBackfillCoordinator coordinator,
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
        var tableNames = tables.Select(t => t.Table.QualifiedName).ToArray();

        await RunDueBackfillsAsync(ct);
        while (!ct.IsCancellationRequested)
        {
            var requested = await store.ListRequestedAsync(tableNames, ct);
            if (requested.Count > 0)
            {
                logger.BackfillRequestsObserved(requested.Count);
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
        foreach (var (table, version) in tables)
        {
            var state = await store.GetAsync(table.QualifiedName, ct);
            var action = DetermineAction(state, version, options);
            if (action == BackfillAction.Skip)
            {
                continue;
            }

            if (action == BackfillAction.Fresh)
            {
                // Reset the cursor so the coordinator starts from the beginning.
                await store.SaveAsync(
                    new BackfillState(table.QualifiedName, BackfillStatus.InProgress, version, null, 0, DateTimeOffset.UtcNow), ct);
            }

            logger.BackfillScheduled(action, table.QualifiedName, version);
            await coordinator.BackfillTableAsync(table, version, ct);
        }
    }

    /// <summary>Pure decision used by the scheduler (separated for testability).</summary>
    public static BackfillAction DetermineAction(BackfillState? state, string? declaredVersion, BackfillSchedulerOptions options)
    {
        if (state is null)
        {
            return options.AutoBackfillNewTables ? BackfillAction.Fresh : BackfillAction.Skip;
        }

        return state.Status switch
        {
            BackfillStatus.Requested => BackfillAction.Fresh,
            BackfillStatus.NotStarted => BackfillAction.Fresh,
            BackfillStatus.InProgress => BackfillAction.Resume,
            BackfillStatus.Completed when options.AutoBackfillOnVersionChange && state.TransformVersion != declaredVersion
                => BackfillAction.Fresh,
            _ => BackfillAction.Skip,
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
}
