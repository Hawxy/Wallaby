using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.UnitTests;

public class FanoutQueueWorkerTests
{
    private sealed class FakeQueue(params FanoutJobRow[] jobs) : IFanoutQueueStore
    {
        private readonly Queue<FanoutJobRow> _due = new(jobs);
        public int Deferred { get; private set; }
        public int Completed { get; private set; }

        public Task EnqueueAsync(ScopedFanoutSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task<FanoutJobRow?> GetNextDueAsync(CancellationToken ct)
            => Task.FromResult(_due.Count > 0 ? _due.Dequeue() : null);
        public Task<long> CountDueAsync(CancellationToken ct) => Task.FromResult((long)_due.Count);
        public Task MarkInProgressAsync(string t, string h, string? c, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(string t, string h, string? c, long r, CancellationToken ct) => Task.CompletedTask;
        public Task CompleteAsync(string t, string h, CancellationToken ct) { Completed++; return Task.CompletedTask; }
        public Task DeferAsync(string t, string h, CancellationToken ct) { Deferred++; return Task.CompletedTask; }
        public Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<FanoutJobRow>>([]);
        public IFanoutQueueSubscription Subscribe() => new NoOpSubscription();
    }

    private sealed class NoOpSubscription : IFanoutQueueSubscription
    {
        public Task WaitForJobAsync(TimeSpan fallbackTimeout, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeBackfillStore : IBackfillStateStore
    {
        public Task<BackfillState?> GetAsync(string t, CancellationToken ct) => Task.FromResult<BackfillState?>(null);
        public Task SaveAsync(BackfillState state, CancellationToken ct) => Task.CompletedTask;
        public Task<IReadOnlyList<BackfillState>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<BackfillState>>([]);
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

        public IFanoutQueueSubscription Subscribe() => new StoppingSubscription(onIdle);

        public Task<long> CountDueAsync(CancellationToken ct) => Task.FromResult(0L);
        public Task EnqueueAsync(ScopedFanoutSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task MarkInProgressAsync(string t, string h, string? c, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(string t, string h, string? c, long r, CancellationToken ct) => Task.CompletedTask;
        public Task CompleteAsync(string t, string h, CancellationToken ct) => Task.CompletedTask;
        public Task DeferAsync(string t, string h, CancellationToken ct) => Task.CompletedTask;
        public Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<FanoutJobRow>>([]);
    }

    private sealed class StoppingSubscription(Action onIdle) : IFanoutQueueSubscription
    {
        public Task WaitForJobAsync(TimeSpan fallbackTimeout, CancellationToken ct)
        {
            onIdle();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // Empty queue (drains immediately) with a fixed due count, stopping the worker at the idle wait.
    private sealed class CountingQueue(long dueCount, Action onIdle) : IFanoutQueueStore
    {
        public Task<FanoutJobRow?> GetNextDueAsync(CancellationToken ct) => Task.FromResult<FanoutJobRow?>(null);
        public Task<long> CountDueAsync(CancellationToken ct) => Task.FromResult(dueCount);
        public IFanoutQueueSubscription Subscribe() => new StoppingSubscription(onIdle);

        public Task EnqueueAsync(ScopedFanoutSpec spec, CancellationToken ct) => Task.CompletedTask;
        public Task MarkInProgressAsync(string t, string h, string? c, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(string t, string h, string? c, long r, CancellationToken ct) => Task.CompletedTask;
        public Task CompleteAsync(string t, string h, CancellationToken ct) => Task.CompletedTask;
        public Task DeferAsync(string t, string h, CancellationToken ct) => Task.CompletedTask;
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
