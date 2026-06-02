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
}
