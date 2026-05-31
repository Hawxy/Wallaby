using Wallaby.Abstractions;
using Wallaby.Internal.Backfill;

namespace EFCore.CDC.UnitTests;

public class BackfillSchedulerTests
{
    private static readonly BackfillSchedulerOptions Defaults = new();

    private static BackfillState State(BackfillStatus status, string? version) =>
        new("public.products", status, version, CursorJson: null, RowsCopied: 0, DateTimeOffset.UtcNow);

    [Test]
    public async Task New_table_is_fresh_when_auto_enabled()
    {
        var action = BackfillScheduler.DetermineAction(state: null, declaredVersion: "v1", Defaults);
        await Assert.That(action).IsEqualTo(BackfillAction.Fresh);
    }

    [Test]
    public async Task New_table_is_skipped_when_auto_disabled()
    {
        var options = new BackfillSchedulerOptions { AutoBackfillNewTables = false };
        var action = BackfillScheduler.DetermineAction(state: null, declaredVersion: "v1", options);
        await Assert.That(action).IsEqualTo(BackfillAction.Skip);
    }

    [Test]
    public async Task Requested_is_fresh()
    {
        var action = BackfillScheduler.DetermineAction(State(BackfillStatus.Requested, "v1"), "v1", Defaults);
        await Assert.That(action).IsEqualTo(BackfillAction.Fresh);
    }

    [Test]
    public async Task In_progress_resumes()
    {
        var action = BackfillScheduler.DetermineAction(State(BackfillStatus.InProgress, "v1"), "v1", Defaults);
        await Assert.That(action).IsEqualTo(BackfillAction.Resume);
    }

    [Test]
    public async Task Completed_same_version_is_skipped()
    {
        var action = BackfillScheduler.DetermineAction(State(BackfillStatus.Completed, "v1"), "v1", Defaults);
        await Assert.That(action).IsEqualTo(BackfillAction.Skip);
    }

    [Test]
    public async Task Completed_changed_version_is_fresh()
    {
        var action = BackfillScheduler.DetermineAction(State(BackfillStatus.Completed, "v1"), "v2", Defaults);
        await Assert.That(action).IsEqualTo(BackfillAction.Fresh);
    }

    [Test]
    public async Task Completed_changed_version_is_skipped_when_auto_version_disabled()
    {
        var options = new BackfillSchedulerOptions { AutoBackfillOnVersionChange = false };
        var action = BackfillScheduler.DetermineAction(State(BackfillStatus.Completed, "v1"), "v2", options);
        await Assert.That(action).IsEqualTo(BackfillAction.Skip);
    }
}
