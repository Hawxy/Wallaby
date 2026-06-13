using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
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
        public Task MarkInProgressAsync(string t, string h, string? c, CancellationToken ct) => Task.CompletedTask;
        public Task SaveProgressAsync(string t, string h, string? c, long r, CancellationToken ct) => Task.CompletedTask;
        public Task CompleteAsync(string t, string h, CancellationToken ct) { Completed++; return Task.CompletedTask; }
        public Task DeferAsync(string t, string h, CancellationToken ct) { Deferred++; return Task.CompletedTask; }
        public Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyList<FanoutJobRow>>([]);
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
        var worker = new FanoutQueueWorker(queue, coordinator, new WallabyModel([]), NullLogger.Instance);

        var ran = await worker.DrainOnceAsync(CancellationToken.None);

        ran.ShouldBe(0);            // nothing actually ran
        queue.Deferred.ShouldBe(1); // it was deferred...
        queue.Completed.ShouldBe(0); // ...not dropped
    }
}
