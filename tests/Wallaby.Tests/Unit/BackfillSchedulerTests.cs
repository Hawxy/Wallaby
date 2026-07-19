using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Tests.Unit;

public class BackfillSchedulerTests
{
    private static readonly BackfillSchedulerOptions Defaults = new();

    private static BackfillState State(BackfillStatus status, string? version, bool purge = false) =>
        new("public.products", status, version, CursorJson: null, RowsCopied: 0, DateTimeOffset.UtcNow, purge);

    private static BackfillDecision Decide(
        BackfillState? state, string? declaredVersion, BackfillSchedulerOptions? options = null,
        bool purgeOnVersionChange = false)
        => BackfillScheduler.Decide(state, declaredVersion, purgeOnVersionChange, options ?? Defaults);

    [Test]
    public void New_table_is_fresh_when_auto_enabled()
    {
        Decide(state: null, declaredVersion: "v1").ShouldBe(new BackfillDecision(BackfillAction.Fresh, Purge: false));
    }

    [Test]
    public void New_table_is_skipped_when_auto_disabled()
    {
        var options = new BackfillSchedulerOptions { AutoBackfillNewTables = false };
        Decide(state: null, declaredVersion: "v1", options).Action.ShouldBe(BackfillAction.Skip);
    }

    [Test]
    public void Requested_is_fresh()
    {
        Decide(State(BackfillStatus.Requested, "v1"), "v1")
            .ShouldBe(new BackfillDecision(BackfillAction.Fresh, Purge: false));
    }

    [Test]
    public void Requested_with_purge_mark_is_fresh_with_purge()
    {
        Decide(State(BackfillStatus.Requested, "v1", purge: true), "v1")
            .ShouldBe(new BackfillDecision(BackfillAction.Fresh, Purge: true));
    }

    [Test]
    public void In_progress_resumes()
    {
        Decide(State(BackfillStatus.InProgress, "v1"), "v1")
            .ShouldBe(new BackfillDecision(BackfillAction.Resume, Purge: false));
    }

    [Test]
    public void In_progress_never_purges_even_when_marked()
    {
        // Re-purging a resumed run would delete the chunks it already delivered.
        Decide(State(BackfillStatus.InProgress, "v1", purge: true), "v1")
            .ShouldBe(new BackfillDecision(BackfillAction.Resume, Purge: false));
    }

    [Test]
    public void Completed_same_version_is_skipped()
    {
        Decide(State(BackfillStatus.Completed, "v1"), "v1").Action.ShouldBe(BackfillAction.Skip);
    }

    [Test]
    public void Completed_changed_version_is_fresh()
    {
        Decide(State(BackfillStatus.Completed, "v1"), "v2")
            .ShouldBe(new BackfillDecision(BackfillAction.Fresh, Purge: false));
    }

    [Test]
    public void Completed_changed_version_purges_when_mapping_opted_in()
    {
        Decide(State(BackfillStatus.Completed, "v1"), "v2", purgeOnVersionChange: true)
            .ShouldBe(new BackfillDecision(BackfillAction.Fresh, Purge: true));
    }

    [Test]
    public void Completed_changed_version_is_skipped_when_auto_version_disabled()
    {
        var options = new BackfillSchedulerOptions { AutoBackfillOnVersionChange = false };
        Decide(State(BackfillStatus.Completed, "v1"), "v2", options, purgeOnVersionChange: true)
            .Action.ShouldBe(BackfillAction.Skip);
    }

    // ---- request loop ----

    // Every pass sees Completed at the declared version (Skip), so the coordinator is never invoked and
    // the loop's behavior is observable purely through the store's event log.
    private sealed class LoopStore(Queue<string[]> requestedPerCheck, Action onWait) : IBackfillStateStore
    {
        public List<string> Events { get; } = [];

        public Task<BackfillState?> GetAsync(string t, CancellationToken ct)
        {
            Events.Add("pass");
            return Task.FromResult<BackfillState?>(
                new BackfillState(t, BackfillStatus.Completed, "v1", null, 0, DateTimeOffset.UtcNow));
        }

        public Task<IReadOnlyList<string>> ListRequestedAsync(IReadOnlyList<string> t, CancellationToken ct)
        {
            Events.Add("check");
            return Task.FromResult<IReadOnlyList<string>>(
                requestedPerCheck.Count > 0 ? requestedPerCheck.Dequeue() : []);
        }

        public INotifySubscription Subscribe() => new WaitSignal(Events, onWait);

        public Task SaveAsync(BackfillState state, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(BackfillState state, CancellationToken ct) => Task.CompletedTask;
        public Task RequestAsync(string t, string? v, bool purge, CancellationToken ct) => Task.CompletedTask;
        public Task<IReadOnlyList<BackfillState>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<BackfillState>>([]);
    }

    private sealed class WaitSignal(List<string> events, Action onWait) : INotifySubscription
    {
        public Task WaitAsync(TimeSpan fallbackTimeout, CancellationToken ct)
        {
            events.Add("wait");
            onWait();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static async Task<List<string>> RunLoopAsync(Queue<string[]> requestedPerCheck)
    {
        using var stop = new CancellationTokenSource();
        var store = new LoopStore(requestedPerCheck, onWait: stop.Cancel);
        var table = new CapturedTable
        {
            EntityClrType = typeof(object),
            Schema = "public",
            TableName = "products",
            Columns = [],
            PrimaryKey = [],
        };

        // Never opened: every pass skips, so the coordinator's data source is untouched.
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var coordinator = new WatermarkBackfillCoordinator(dataSource, store, NullLogger.Instance);
        var scheduler = new BackfillScheduler(
            [new BackfillTable(table, "v1", PurgeOnVersionChange: false, PurgeTargets: [])],
            store, coordinator,
            new SinkPurgeRunner(new Dictionary<string, ISink>(), WallabyInstrumentation.NoOp, NullLogger.Instance),
            new BackfillSchedulerOptions(), NullLogger.Instance);

        await scheduler.RunAsync(TimeSpan.FromSeconds(30), stop.Token);
        return store.Events;
    }

    [Test]
    public async Task The_loop_idles_when_nothing_is_requested()
    {
        var events = await RunLoopAsync(new Queue<string[]>());

        events.ShouldBe(["pass", "check", "wait"]);
    }

    [Test]
    public async Task A_request_observed_after_the_initial_pass_triggers_another_pass()
    {
        var events = await RunLoopAsync(new Queue<string[]>([["public.products"]]));

        events.Count(e => e == "pass").ShouldBe(2);
    }

    [Test]
    public async Task The_loop_checks_for_requests_before_waiting()
    {
        // A request that landed during the initial pass is served straight away, never stranded at a wait.
        var events = await RunLoopAsync(new Queue<string[]>([["public.products"]]));

        events.IndexOf("wait").ShouldBe(events.Count - 1);
        events.Take(events.IndexOf("wait")).ShouldBe(["pass", "check", "pass", "check"]);
    }
}
