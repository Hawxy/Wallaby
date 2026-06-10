using System.Collections.Concurrent;
using System.Diagnostics;
using EFCore.CDC.TestModel;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.State;
using Wallaby.Model;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;

namespace Wallaby.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class FanoutScalabilityTests(PostgresFixture pg)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Large_transaction_is_dispatched_in_bounded_batches()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        harness.MaxBatchSize = 5;
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new CdcDocument { ["name"] = p.Name });

        var spans = new ConcurrentBag<string>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == WallabyInstrumentation.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => spans.Add(activity.OperationName),
        };
        ActivitySource.AddActivityListener(listener);

        await harness.SelfConfigureAsync();
        var categoryId = await harness.Db.AddCategoryAsync();
        var names = Enumerable.Range(0, 12).Select(i => $"p{i}").ToArray();

        await harness.StartAsync();
        try
        {
            await harness.Db.AddProductsAsync(categoryId, names); // one transaction => 12 changes
            await harness.WaitUntilAsync(
                () => capture.For("products").Select(r => r.DocumentId).Distinct().Count() >= 12, Timeout);
        }
        finally
        {
            await harness.StopAsync();
        }

        // 12 changes in one transaction with MaxBatchSize 5 => at least 3 sink.deliver batches (5 + 5 + 2).
        await Assert.That(spans.Count(n => n == "sink.deliver")).IsGreaterThanOrEqualTo(3);
        await Assert.That(capture.For("products").Select(r => r.DocumentId).Distinct().Count()).IsEqualTo(12);
    }

    [Test]
    public async Task Multiple_dependent_changes_in_one_transaction_fan_out_each_primary_once()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new CdcDocument { ["name"] = p.Name });
        harness.DependsOn<Product, Category?>(p => p.Category);

        // Seed before self-config so these inserts are not streamed — only the rename below is.
        var cat1 = await harness.Db.AddCategoryAsync("Cat1");
        var cat2 = await harness.Db.AddCategoryAsync("Cat2");
        await harness.Db.AddProductAsync(cat1, "p1");
        await harness.Db.AddProductAsync(cat1, "p2");
        await harness.Db.AddProductAsync(cat2, "p3");
        await harness.Db.AddProductAsync(cat2, "p4");

        await harness.SelfConfigureAsync();

        using var synthetic = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.dependent.synthetic");

        await harness.StartAsync();
        try
        {
            // Both category renames in ONE transaction => one consolidated query covering all four products.
            await harness.Db.SetCategoryNamesAsync([(cat1, "Cat1b"), (cat2, "Cat2b")]);
            await harness.WaitUntilAsync(
                () => capture.For("products").Select(r => r.DocumentId).Distinct().Count() >= 4, Timeout);
        }
        finally
        {
            await harness.StopAsync();
        }

        var productRecords = capture.For("products").ToList();
        // Each affected product is emitted exactly once (no duplicates from the consolidated fan-out).
        await Assert.That(productRecords.Count).IsEqualTo(4);
        await Assert.That(productRecords.Select(r => r.DocumentId).Distinct().Count()).IsEqualTo(4);
        await Assert.That(synthetic.GetMeasurementSnapshot().Sum(m => m.Value)).IsGreaterThanOrEqualTo(4L);
    }

    [Test]
    public async Task Primary_changed_with_its_dependent_in_one_transaction_is_emitted_once()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new CdcDocument { ["name"] = p.Name });
        harness.DependsOn<Product, Category?>(p => p.Category);

        // Seed before self-config so these inserts are not streamed — only the combined rename below is.
        var cat = await harness.Db.AddCategoryAsync("Cat");
        var p1 = await harness.Db.AddProductAsync(cat, "p1");
        var p2 = await harness.Db.AddProductAsync(cat, "p2");

        await harness.SelfConfigureAsync();

        await harness.StartAsync();
        try
        {
            // Rename the category and p1 in the same transaction. p1 is emitted once (its live change wins,
            // and is excluded from the fan-out); p2 is emitted once via the fan-out. p1 is NOT emitted twice.
            await harness.Db.RenameCategoryAndProductAsync(cat, "Cat2", p1, "p1b");
            await harness.WaitUntilAsync(
                () => capture.For("products").Select(r => r.DocumentId).Distinct().Count() >= 2, Timeout);
        }
        finally
        {
            await harness.StopAsync();
        }

        var records = capture.For("products").ToList();
        await Assert.That(records.Count).IsEqualTo(2);
        await Assert.That(records.Select(r => r.DocumentId).Distinct().Count()).IsEqualTo(2);
    }

    [Test]
    public async Task Wide_fanout_offloads_the_tail_coalesces_repeat_triggers_and_drains()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        harness.MaxBatchSize = 5;
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new CdcDocument { ["name"] = p.Name });
        harness.DependsOn<Product, Category?>(p => p.Category);

        // Seed before self-config so the inserts are not streamed; only the renames below fan out.
        var cat = await harness.Db.AddCategoryAsync("Cat");
        await harness.Db.AddProductsAsync(cat, Enumerable.Range(0, 12).Select(i => $"p{i}").ToArray());

        await harness.SelfConfigureAsync();
        // The wallaby.fanout_queue is shared across tests in this session; isolate this test's count.
        await harness.ClearFanoutQueueAsync();

        using var synthetic = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.dependent.synthetic");

        await harness.StartAsync();
        try
        {
            // First rename: inline first page (5) + offload the remaining 7 as one queued job.
            await harness.Db.SetCategoryNameAsync(cat, "Cat-a");
            await harness.WaitUntilAsync(async () => await harness.PendingFanoutJobCountAsync() == 1, Timeout);

            // Second rename of the same category coalesces onto the same (table, lookup) job.
            await harness.Db.SetCategoryNameAsync(cat, "Cat-b");
            await harness.WaitUntilAsync(() => synthetic.GetMeasurementSnapshot().Sum(m => m.Value) >= 10, Timeout);

            await Assert.That(await harness.PendingFanoutJobCountAsync()).IsEqualTo(1);

            // The trigger transactions are acknowledged (slot advances) while the tail is still queued:
            // both renames re-emitted the same inline first page, so only 5 distinct products are delivered.
            await harness.WaitUntilAsync(() => harness.LastAcknowledgedLsn > 0, Timeout);
            await Assert.That(capture.For("products").Select(r => r.DocumentId).Distinct().Count()).IsEqualTo(5);

            // Draining the offloaded job re-emits the remaining 7, completing the fan-out.
            await Assert.That(await harness.DrainFanoutAsync()).IsEqualTo(1);
            await harness.WaitUntilAsync(
                () => capture.For("products").Select(r => r.DocumentId).Distinct().Count() >= 12, Timeout);
        }
        finally
        {
            await harness.StopAsync();
        }
    }

    [Test]
    public async Task Fanout_queue_store_tracks_resume_cursor_and_coalesces()
    {
        await using (var conn = await pg.DataSource.OpenConnectionAsync())
        {
            await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
        }
        await PgExec.ExecuteAsync(pg.DataSource, "DELETE FROM wallaby.fanout_queue", CancellationToken.None);

        var store = new PostgresFanoutQueueStore(pg.DataSource);
        var table = new CapturedTable
        {
            EntityClrType = typeof(Product),
            Schema = "public",
            TableName = "products",
            Columns = [],
            PrimaryKey = [],
        };
        var spec = new ScopedFanoutSpec(table, ["category_id"], [new object?[] { 4242 }]);

        await store.EnqueueAsync(spec, CancellationToken.None);
        var due = await store.GetNextDueAsync(CancellationToken.None);
        await Assert.That(due).IsNotNull();
        await Assert.That(due!.Status).IsEqualTo(BackfillStatus.Requested);

        // Marking in progress with a cursor makes it a resumable in-progress job.
        await store.MarkInProgressAsync(
            due.TableQualified, due.LookupHash, KeysetCodec.Serialize([(object?)42]), CancellationToken.None);
        var resumed = await store.GetNextDueAsync(CancellationToken.None);
        await Assert.That(resumed!.Status).IsEqualTo(BackfillStatus.InProgress);
        await Assert.That(resumed.CursorJson).IsNotNull();

        // Completing it (guarded on InProgress) removes it from the due set.
        await store.CompleteAsync(due.TableQualified, due.LookupHash, CancellationToken.None);
        await Assert.That(await store.GetNextDueAsync(CancellationToken.None)).IsNull();

        // A repeat trigger for the same lookup re-arms the SAME row rather than adding a second.
        await store.EnqueueAsync(spec, CancellationToken.None);
        var rearmed = await store.GetNextDueAsync(CancellationToken.None);
        await Assert.That(rearmed!.Status).IsEqualTo(BackfillStatus.Requested);
        await Assert.That((await store.ListAsync(CancellationToken.None)).Count(j => j.LookupHash == due.LookupHash))
            .IsEqualTo(1);
    }
}
