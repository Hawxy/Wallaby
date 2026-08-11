using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Tests.Unit;

public class FanoutQueueWorkerTests
{
    // Port 1 refuses immediately, so a test that intentionally reaches the coordinator fails fast.
    private const string UnreachableConnectionString =
        "Host=127.0.0.1;Port=1;Username=u;Password=p;Database=d;Timeout=2";

    private sealed class FakeQueue(params FanoutJobRow[] jobs) : IFanoutQueueStore
    {
        private readonly Queue<FanoutJobRow> _due = new(jobs);
        public int Deferred { get; private set; }
        public int Completed { get; private set; }
        public List<string> Failed { get; } = [];

        public Task EnqueueAsync(ScopedFanoutSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task<FanoutJobRow?> GetNextDueAsync(CancellationToken ct)
            => Task.FromResult(_due.Count > 0 ? _due.Dequeue() : null);
        public Task<long> CountDueAsync(CancellationToken ct) => Task.FromResult((long)_due.Count);
        public Task<int> MaxAttemptsAsync(CancellationToken ct) => Task.FromResult(0);
        public Task MarkInProgressAsync(string t, string h, string? c, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(string t, string h, string? c, long r, CancellationToken ct) => Task.CompletedTask;
        public Task CompleteAsync(string t, string h, CancellationToken ct) { Completed++; return Task.CompletedTask; }
        public Task DeferAsync(string t, string h, TimeSpan d, CancellationToken ct) { Deferred++; return Task.CompletedTask; }
        public Task FailAsync(string t, string h, string error, CancellationToken ct) { Failed.Add(t); return Task.CompletedTask; }
        public Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<FanoutJobRow>>([]);
        public INotifySubscription Subscribe() => new NoOpSubscription();
    }

    private sealed class NoOpSubscription : INotifySubscription
    {
        public Task WaitAsync(TimeSpan fallbackTimeout, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeBackfillStore : IBackfillStateStore
    {
        public Task<BackfillState?> GetAsync(string t, CancellationToken ct) => Task.FromResult<BackfillState?>(null);
        public Task SaveAsync(BackfillState state, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(string t, BackfillStatus s, string? c, long r, CancellationToken ct)
            => Task.CompletedTask;
        public Task RequestAsync(string t, bool purge, CancellationToken ct) => Task.CompletedTask;
        public Task<bool> CancelRequestAsync(string t, CancellationToken ct) => Task.FromResult(false);
        public Task<IReadOnlyList<string>> ListRequestedAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<string>>([]);
        public Task<IReadOnlyList<BackfillState>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<BackfillState>>([]);
        public INotifySubscription Subscribe() => new NoOpSubscription();
    }

    [Test]
    public async Task Job_for_a_table_not_in_the_model_is_deferred_not_dropped()
    {
        var queue = new FakeQueue(new FanoutJobRow(
            "public.nonexistent", "hash1", BackfillStatus.Requested, ["col"], "[]", null, 0));

        // The coordinator/store are never invoked on the divergent path, so a never-opened data source is fine.
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var coordinator = new WatermarkBackfillCoordinator(dataSource, new FakeBackfillStore(), NullLogger.Instance);
        var worker = new FanoutQueueWorker(queue, coordinator, new WallabyModel([]), NullLogger.Instance, TimeSpan.FromSeconds(1));

        var ran = await worker.DrainOnceAsync(CancellationToken.None);

        ran.ShouldBe(0);            // nothing actually ran
        queue.Deferred.ShouldBe(1); // it was deferred...
        queue.Completed.ShouldBe(0); // ...not dropped
    }

    [Test]
    public async Task A_failing_job_is_recorded_and_does_not_starve_the_jobs_behind_it()
    {
        // Both jobs die when the coordinator opens its unreachable connection. What matters is that the
        // second is still attempted: an unhandled failure would abort the drain and starve everything
        // behind the first.
        var queue = new FakeQueue(
            new FanoutJobRow("public.widgets", "hash1", BackfillStatus.Requested, ["col"], "[[1]]", null, 0),
            new FanoutJobRow("public.widgets", "hash2", BackfillStatus.Requested, ["col"], "[[2]]", null, 0, Attempts: 4));

        var status = new WallabyStatus();
        await using var dataSource = NpgsqlDataSource.Create(UnreachableConnectionString);
        var coordinator = new WatermarkBackfillCoordinator(dataSource, new FakeBackfillStore(), NullLogger.Instance);
        var worker = new FanoutQueueWorker(
            queue, coordinator, new WallabyModel([WidgetsTable()]), NullLogger.Instance, TimeSpan.FromSeconds(1),
            status);

        var ran = await worker.DrainOnceAsync(CancellationToken.None);

        ran.ShouldBe(0);
        queue.Failed.Count.ShouldBe(2);  // both were attempted and both were backed off
        queue.Deferred.ShouldBe(0);      // a failure is not a model divergence
        // The counter reflects the worst job's persisted streak (4 prior failures + this one), not a
        // per-pass tally.
        status.Current.ConsecutiveFanoutFailures.ShouldBe(5);
    }

    [Test]
    public async Task A_job_with_unreadable_lookup_values_is_dropped_not_retried()
    {
        var queue = new FakeQueue(new FanoutJobRow(
            "public.widgets", "hash1", BackfillStatus.Requested, ["col"], "not json", null, 0));

        // The coordinator is never reached: the values fail to deserialize first.
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var coordinator = new WatermarkBackfillCoordinator(dataSource, new FakeBackfillStore(), NullLogger.Instance);
        var worker = new FanoutQueueWorker(
            queue, coordinator, new WallabyModel([WidgetsTable()]), NullLogger.Instance, TimeSpan.FromSeconds(1));

        var ran = await worker.DrainOnceAsync(CancellationToken.None);

        ran.ShouldBe(0);
        queue.Completed.ShouldBe(1); // dropped...
        queue.Failed.ShouldBeEmpty(); // ...not backed off
    }

    [Test]
    public async Task A_backed_off_failing_job_holds_the_streak_through_a_clean_pass()
    {
        var status = new WallabyStatus();
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var coordinator = new WatermarkBackfillCoordinator(dataSource, new FakeBackfillStore(), NullLogger.Instance);

        // A clean pass while a job with 3 recorded failures sits in backoff: the streak must hold, or
        // healthy traffic would keep the health check green around a poison job.
        using (var stop = new CancellationTokenSource())
        {
            var queue = new CountingQueue(dueCount: 0, onIdle: stop.Cancel, maxAttempts: 3);
            await new FanoutQueueWorker(
                queue, coordinator, new WallabyModel([]), NullLogger.Instance, TimeSpan.FromSeconds(1), status)
                .RunAsync(stop.Token);
        }
        status.Current.ConsecutiveFanoutFailures.ShouldBe(3);

        // The job's row disappearing (it finally completed) clears the streak on the next pass.
        using (var stop = new CancellationTokenSource())
        {
            var queue = new CountingQueue(dueCount: 0, onIdle: stop.Cancel);
            await new FanoutQueueWorker(
                queue, coordinator, new WallabyModel([]), NullLogger.Instance, TimeSpan.FromSeconds(1), status)
                .RunAsync(stop.Token);
        }
        status.Current.ConsecutiveFanoutFailures.ShouldBe(0);
    }

    private static CapturedTable WidgetsTable()
    {
        var id = new CapturedColumn
        {
            PropertyName = "Id", ColumnName = "id", ClrType = typeof(int), IsPrimaryKey = true,
        };
        return new CapturedTable
        {
            EntityClrType = typeof(object),
            Schema = "public",
            TableName = "widgets",
            Columns =
            [
                id,
                new CapturedColumn
                {
                    PropertyName = "Col", ColumnName = "col", ClrType = typeof(int), IsPrimaryKey = false,
                },
            ],
            PrimaryKey = [id],
        };
    }

    // Throws on the first pass, is empty (healthy) on the second, and stops the worker at the idle wait.
    private sealed class ThrowOnceQueue(Action onHealthyPass, Action onIdle) : IFanoutQueueStore
    {
        private int _calls;

        public Task<FanoutJobRow?> GetNextDueAsync(CancellationToken ct)
        {
            if (++_calls == 1)
            {
                throw new InvalidOperationException("poison pass");
            }
            onHealthyPass();
            return Task.FromResult<FanoutJobRow?>(null);
        }

        public INotifySubscription Subscribe() => new StoppingSubscription(onIdle);

        public Task<long> CountDueAsync(CancellationToken ct) => Task.FromResult(0L);
        public Task EnqueueAsync(ScopedFanoutSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task MarkInProgressAsync(string t, string h, string? c, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(string t, string h, string? c, long r, CancellationToken ct) => Task.CompletedTask;
        public Task CompleteAsync(string t, string h, CancellationToken ct) => Task.CompletedTask;
        public Task DeferAsync(string t, string h, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task FailAsync(string t, string h, string error, CancellationToken ct) => Task.CompletedTask;
        public Task<int> MaxAttemptsAsync(CancellationToken ct) => Task.FromResult(0);
        public Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<FanoutJobRow>>([]);
    }

    private sealed class StoppingSubscription(Action onIdle) : INotifySubscription
    {
        public Task WaitAsync(TimeSpan fallbackTimeout, CancellationToken ct)
        {
            onIdle();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // Empty queue (drains immediately) with fixed due/attempt counts, stopping the worker at the idle wait.
    private sealed class CountingQueue(long dueCount, Action onIdle, int maxAttempts = 0) : IFanoutQueueStore
    {
        public Task<FanoutJobRow?> GetNextDueAsync(CancellationToken ct) => Task.FromResult<FanoutJobRow?>(null);
        public Task<long> CountDueAsync(CancellationToken ct) => Task.FromResult(dueCount);
        public Task<int> MaxAttemptsAsync(CancellationToken ct) => Task.FromResult(maxAttempts);
        public INotifySubscription Subscribe() => new StoppingSubscription(onIdle);

        public Task EnqueueAsync(ScopedFanoutSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task MarkInProgressAsync(string t, string h, string? c, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(string t, string h, string? c, long r, CancellationToken ct) => Task.CompletedTask;
        public Task CompleteAsync(string t, string h, CancellationToken ct) => Task.CompletedTask;
        public Task DeferAsync(string t, string h, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task FailAsync(string t, string h, string error, CancellationToken ct) => Task.CompletedTask;
        public Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<FanoutJobRow>>([]);
    }

    [Test]
    public async Task Drain_pass_records_the_fanout_queue_depth_gauge()
    {
        using var instr = new WallabyInstrumentation();
        using var depth = new Microsoft.Extensions.Diagnostics.Metrics.Testing.MetricCollector<long>(
            instr.Meter, "wallaby.fanout.queue.depth");
        using var stop = new CancellationTokenSource();
        var queue = new CountingQueue(dueCount: 3, onIdle: stop.Cancel);

        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var coordinator = new WatermarkBackfillCoordinator(dataSource, new FakeBackfillStore(), NullLogger.Instance);
        var worker = new FanoutQueueWorker(
            queue, coordinator, new WallabyModel([]), NullLogger.Instance, TimeSpan.FromSeconds(1),
            instrumentation: instr);

        await worker.RunAsync(stop.Token);

        depth.RecordObservableInstruments();
        depth.LastMeasurement.ShouldNotBeNull();
        depth.LastMeasurement.Value.ShouldBe(3);
    }

    [Test]
    public async Task Failed_pass_records_a_fanout_failure_and_a_healthy_pass_resets_it()
    {
        var status = new WallabyStatus();
        using var stop = new CancellationTokenSource();
        var failuresSeenOnHealthyPass = -1;

        // Capture the counter as the healthy pass starts (before its reset), and stop once the worker idles.
        var queue = new ThrowOnceQueue(
            onHealthyPass: () => failuresSeenOnHealthyPass = status.Current.ConsecutiveFanoutFailures,
            onIdle: stop.Cancel);

        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var coordinator = new WatermarkBackfillCoordinator(dataSource, new FakeBackfillStore(), NullLogger.Instance);
        var worker = new FanoutQueueWorker(
            queue, coordinator, new WallabyModel([]), NullLogger.Instance, TimeSpan.FromSeconds(1), status);

        await worker.RunAsync(stop.Token);

        failuresSeenOnHealthyPass.ShouldBe(1); // the failed pass was recorded...
        status.Current.ConsecutiveFanoutFailures.ShouldBe(0); // ...and the healthy pass reset it
        status.Current.LastError.ShouldBe("InvalidOperationException: poison pass");
    }
}
