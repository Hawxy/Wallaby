using Wallaby.Abstractions;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Cross-transaction micro-batching: a burst of small committed transactions is delivered and
/// acknowledged in coalesced batches (order preserved), a mid-batch failure acknowledges nothing, and
/// restart semantics hold with batching on.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class MicroBatchingTests(TestModelPostgresFixture pg)
{
    private static string? NameOf(SinkRecord r) => r.Document?.GetValueOrDefault("Name") as string;

    [Test]
    public async Task A_burst_of_tiny_transactions_coalesces_deliveries_and_acks()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var capture = harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        // Commit the burst before the pipeline starts, so the whole backlog is waiting in the slot and
        // the batcher's greedy drain has buffered transactions to coalesce.
        var categoryId = await harness.Db.AddCategoryAsync();
        var expected = new List<string>();
        for (var i = 0; i < 30; i++)
        {
            var name = $"burst_{i:D2}";
            expected.Add(name);
            await harness.Db.AddProductAsync(categoryId, name);
        }

        using var activities = new ActivityCapture(harness.Instrumentation);
        await harness.RunUntilAsync(() =>
            capture.For("products").Count(r => NameOf(r)?.StartsWith("burst_") is true) >= 30);

        // Commit order is preserved across coalesced batches.
        var names = capture.For("products").Select(NameOf).Where(n => n?.StartsWith("burst_") is true).ToList();
        names.ShouldBe(expected);

        // 30 source transactions produced fewer acknowledgements — the point of micro-batching.
        activities.All("ack").Count.ShouldBeLessThan(30);
        activities.All("sink.deliver").Count.ShouldBeLessThan(30);
    }

    [Test]
    public async Task Restart_under_a_burst_neither_loses_nor_duplicates()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var capture = harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        for (var i = 0; i < 10; i++)
        {
            await harness.Db.AddProductAsync(categoryId, $"one_{i:D2}");
        }

        // Run 1: consume the burst and ensure the last commit was acknowledged before stopping.
        await harness.RunUntilAsync(() =>
        {
            var last = capture.For("products").FirstOrDefault(r => NameOf(r) == "one_09");
            return last is not null && harness.LastAcknowledgedLsn >= last.Metadata.CommitLsn;
        });

        // Run 2: a fresh stream on the same slot resumes after the whole first burst.
        capture.Clear();
        for (var i = 0; i < 10; i++)
        {
            await harness.Db.AddProductAsync(categoryId, $"two_{i:D2}");
        }
        await harness.RunUntilAsync(() => capture.For("products").Count(r => NameOf(r)?.StartsWith("two_") is true) >= 10);

        var run2 = capture.For("products").Select(NameOf).Where(n => n is not null).ToList();
        run2.ShouldNotContain(n => n!.StartsWith("one_")); // no re-delivery
        run2.Where(n => n!.StartsWith("two_")).GroupBy(n => n).ShouldAllBe(g => g.Count() == 1); // no duplicates
    }

    [Test]
    public async Task A_failure_mid_batch_acknowledges_nothing_and_redelivers_the_whole_batch()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var poison = new PoisonToggleSink();
        harness.AddSink(poison);
        await harness.SelfConfigureAsync();

        // Three tiny transactions that will coalesce into one batch; the middle one poisons the sink.
        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "ok_a");
        await harness.Db.AddProductAsync(categoryId, "poison");
        await harness.Db.AddProductAsync(categoryId, "ok_b");

        await Should.ThrowAsync<Exception>(() => harness.RunUntilAsync(() => false, TimeSpan.FromSeconds(30)));
        harness.LastAcknowledgedLsn.ShouldBe(0ul); // nothing in the failed batch was acknowledged

        // A healthy retry redelivers the entire batch, poison transaction included.
        poison.Armed = false;
        await harness.RunUntilAsync(() =>
        {
            var names = poison.Delivered;
            return names.Contains("ok_a") && names.Contains("poison") && names.Contains("ok_b");
        });
    }

    private sealed class PoisonToggleSink : ISink
    {
        private readonly List<string> _delivered = [];

        public string Name => "poison-toggle";
        public volatile bool Armed = true;

        public IReadOnlyList<string> Delivered
        {
            get { lock (_delivered) { return [.. _delivered]; } }
        }

        public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
        {
            var names = batch.Records.Select(NameOf).Where(n => n is not null).Select(n => n!).ToList();
            if (Armed && names.Contains("poison"))
            {
                return Task.FromResult(DeliveryResult.Permanent("poisoned record"));
            }
            lock (_delivered)
            {
                _delivered.AddRange(names);
            }
            return Task.FromResult(DeliveryResult.Success);
        }
    }
}
