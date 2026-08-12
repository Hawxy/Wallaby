using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Testing;
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

    [Test]
    public void Cancelled_is_skipped()
    {
        Decide(State(BackfillStatus.Cancelled, "v1"), "v1").Action.ShouldBe(BackfillAction.Skip);
    }

    [Test]
    public void Cancelled_is_skipped_even_on_a_version_change()
    {
        Decide(State(BackfillStatus.Cancelled, "v1", purge: true), "v2", purgeOnVersionChange: true)
            .ShouldBe(new BackfillDecision(BackfillAction.Skip, Purge: false));
    }

    // ---- per-table failure isolation ----

    private sealed class RecordingStore(Func<string, BackfillState?> stateFor) : IBackfillStateStore
    {
        public List<string> Saved { get; } = [];
        public List<string> Failed { get; } = [];
        public DateTimeOffset NextAttempt { get; } = DateTimeOffset.UtcNow.AddSeconds(5);

        public Task<BackfillState?> GetAsync(string t, CancellationToken ct) => Task.FromResult(stateFor(t));
        public Task SaveAsync(BackfillState state, CancellationToken ct)
        {
            Saved.Add(state.TableQualifiedName);
            return Task.CompletedTask;
        }
        public Task<DateTimeOffset> FailAsync(string t, string error, CancellationToken ct)
        {
            Failed.Add(t);
            return Task.FromResult(NextAttempt);
        }

        public Task SaveProgressAsync(string t, BackfillStatus s, string? c, long r, CancellationToken ct)
            => Task.CompletedTask;
        public Task RequestAsync(string t, bool purge, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> CancelRequestAsync(string t, CancellationToken ct) => Task.FromResult(false);
        public Task ClearFailureAsync(string t, CancellationToken ct) => Task.CompletedTask;
        public Task<int> MaxAttemptsAsync(CancellationToken ct) => Task.FromResult(0);
        public Task<IReadOnlyList<string>> ListRequestedAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<string>>([]);
        public Task<IReadOnlyList<BackfillState>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<BackfillState>>([]);
        public INotifySubscription Subscribe() => new WaitSignal([], () => { });
    }

    private static BackfillScheduler SchedulerFor(RecordingStore store, out NpgsqlDataSource dataSource)
    {
        CapturedTable Table(string name) => new()
        {
            EntityClrType = typeof(object),
            Schema = "public",
            TableName = name,
            Columns = [],
            PrimaryKey = [],
        };

        // Port 1 refuses connections, so any coordinator run fails fast.
        dataSource = NpgsqlDataSource.Create("Host=localhost;Port=1;Username=u;Password=p;Database=d;Timeout=1");
        var coordinator = new WatermarkBackfillCoordinator(dataSource, store, NullLogger.Instance);
        return new BackfillScheduler(
            [
                new BackfillTable(Table("products"), "v1", PurgeOnVersionChange: false, PurgeTargets: []),
                new BackfillTable(Table("orders"), "v1", PurgeOnVersionChange: false, PurgeTargets: []),
            ],
            store, coordinator,
            new SinkPurgeRunner(new Dictionary<string, ISink>(), WallabyInstrumentation.NoOp, NullLogger.Instance),
            new BackfillSchedulerOptions(), NullLogger.Instance);
    }

    [Test]
    public async Task A_failing_table_backs_off_alone_and_the_pass_continues()
    {
        var store = new RecordingStore(_ => null); // both tables are new => Fresh, and both runs fail
        var scheduler = SchedulerFor(store, out var dataSource);
        await using var _ = dataSource;

        var nextRetryAt = await scheduler.RunDueBackfillsAsync(CancellationToken.None);

        // The first table's failure did not abort the pass: both ran, both recorded their own backoff.
        store.Saved.ShouldBe(["public.products", "public.orders"]);
        store.Failed.ShouldBe(["public.products", "public.orders"]);
        nextRetryAt.ShouldBe(store.NextAttempt);
    }

    [Test]
    public async Task A_table_in_backoff_is_left_until_due()
    {
        var notBefore = DateTimeOffset.UtcNow.AddMinutes(5);
        var store = new RecordingStore(t => new BackfillState(
            t, BackfillStatus.Requested, "v1", CursorJson: null, RowsCopied: 0, DateTimeOffset.UtcNow,
            Purge: false, Attempts: 1, NextAttemptAt: notBefore, LastError: "boom"));
        var scheduler = SchedulerFor(store, out var dataSource);
        await using var _ = dataSource;

        var nextRetryAt = await scheduler.RunDueBackfillsAsync(CancellationToken.None);

        // Requested but backed off: nothing runs until the backoff expires.
        store.Saved.ShouldBeEmpty();
        store.Failed.ShouldBeEmpty();
        nextRetryAt.ShouldBe(notBefore);
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

        public Task<IReadOnlyList<string>> ListRequestedAsync(CancellationToken ct)
        {
            Events.Add("check");
            return Task.FromResult<IReadOnlyList<string>>(
                requestedPerCheck.Count > 0 ? requestedPerCheck.Dequeue() : []);
        }

        public Task<bool> CancelRequestAsync(string t, CancellationToken ct) => Task.FromResult(false);

        public INotifySubscription Subscribe() => new WaitSignal(Events, onWait);

        public Task SaveAsync(BackfillState state, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(string t, BackfillStatus s, string? c, long r, CancellationToken ct)
            => Task.CompletedTask;
        public Task RequestAsync(string t, bool purge, CancellationToken ct) => Task.CompletedTask;
        public Task<DateTimeOffset> FailAsync(string t, string error, CancellationToken ct)
            => Task.FromResult(DateTimeOffset.UtcNow.AddSeconds(5));
        public Task ClearFailureAsync(string t, CancellationToken ct) => Task.CompletedTask;
        public Task<int> MaxAttemptsAsync(CancellationToken ct) => Task.FromResult(0);
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

    private static async Task<List<string>> RunLoopAsync(
        Queue<string[]> requestedPerCheck, ILogger? logger = null, int stopAfterWaits = 1)
    {
        using var stop = new CancellationTokenSource();
        var waits = 0;
        var store = new LoopStore(requestedPerCheck, onWait: () =>
        {
            if (++waits >= stopAfterWaits)
            {
                stop.Cancel();
            }
        });
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
            new BackfillSchedulerOptions(), logger ?? NullLogger.Instance);

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

    [Test]
    public async Task A_request_for_an_unmapped_table_warns_once_and_triggers_no_pass()
    {
        var logger = new FakeLogger();
        // Two checks observe the same unknown name; the loop idles through the first wait.
        var events = await RunLoopAsync(
            new Queue<string[]>([["public.mistyped"], ["public.mistyped"]]), logger, stopAfterWaits: 2);

        // The unknown request never runs a scheduler pass; the initial pass is the only one.
        events.Count(e => e == "pass").ShouldBe(1);

        var warnings = logger.Collector.GetSnapshot().Where(r => r.Level == LogLevel.Warning).ToList();
        warnings.Count.ShouldBe(1);
        warnings[0].Message.ShouldContain("public.mistyped");
    }
}
