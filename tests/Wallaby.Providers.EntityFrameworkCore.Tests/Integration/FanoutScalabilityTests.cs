using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.Internal;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.State;
using Wallaby.Model;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class FanoutScalabilityTests(TestModelPostgresFixture pg)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Large_transaction_is_dispatched_in_bounded_batches()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        harness.MaxBatchSize = 5;
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new WallabyDocument { ["name"] = p.Name });

        using var activities = new ActivityCapture(harness.Instrumentation);

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
        activities.OperationNames.Count(n => n == "sink.deliver").ShouldBeGreaterThanOrEqualTo(3);
        capture.For("products").Select(r => r.DocumentId).Distinct().Count().ShouldBe(12);
    }

    [Test]
    public async Task Multiple_dependent_changes_in_one_transaction_fan_out_each_primary_once()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new WallabyDocument { ["name"] = p.Name });
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
        using var activities = new ActivityCapture(harness.Instrumentation);

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
        productRecords.Count.ShouldBe(4);
        productRecords.Select(r => r.DocumentId).Distinct().Count().ShouldBe(4);
        synthetic.GetMeasurementSnapshot().Sum(m => m.Value).ShouldBeGreaterThanOrEqualTo(4L);

        // The fan-out's route span is tagged so it is distinguishable from the live category change.
        activities.All("route").ShouldContain(a => (string?)a.GetTagItem("wallaby.source") == "fanout");
    }

    [Test]
    public async Task Transaction_with_no_dependent_change_delivers_without_fanout()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new WallabyDocument { ["name"] = p.Name });
        harness.DependsOn<Product, Category?>(p => p.Category);

        var cat = await harness.Db.AddCategoryAsync("Cat");

        await harness.SelfConfigureAsync();
        await harness.ClearFanoutQueueAsync();

        using var synthetic = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.dependent.synthetic");

        await harness.StartAsync();
        try
        {
            // Only the product changes — the dependent (categories) table is untouched, so no fan-out runs.
            await harness.Db.AddProductAsync(cat, "p1");
            await harness.WaitUntilAsync(() => capture.For("products").Any(), Timeout);
            (await harness.PendingFanoutJobCountAsync()).ShouldBe(0);
        }
        finally
        {
            await harness.StopAsync();
        }

        capture.For("products").Select(r => r.DocumentId).Distinct().Count().ShouldBe(1);
        synthetic.GetMeasurementSnapshot().Sum(m => m.Value).ShouldBe(0L);
    }

    [Test]
    public async Task Primary_changed_with_its_dependent_in_one_transaction_is_emitted_once()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new WallabyDocument { ["name"] = p.Name });
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
        records.Count.ShouldBe(2);
        records.Select(r => r.DocumentId).Distinct().Count().ShouldBe(2);
    }

    [Test]
    public async Task Wide_fanout_offloads_the_tail_coalesces_repeat_triggers_and_drains()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        harness.MaxBatchSize = 5;
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new WallabyDocument { ["name"] = p.Name });
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

            (await harness.PendingFanoutJobCountAsync()).ShouldBe(1);

            // The trigger transactions are acknowledged (slot advances) while the tail is still queued:
            // both renames re-emitted the same inline first page, so only 5 distinct products are delivered.
            await harness.WaitUntilAsync(() => harness.LastAcknowledgedLsn > 0, Timeout);
            capture.For("products").Select(r => r.DocumentId).Distinct().Count().ShouldBe(5);

            // Draining the offloaded job re-emits the remaining 7, completing the fan-out.
            (await harness.DrainFanoutAsync()).ShouldBe(1);
            await harness.WaitUntilAsync(
                () => capture.For("products").Select(r => r.DocumentId).Distinct().Count() >= 12, Timeout);
        }
        finally
        {
            await harness.StopAsync();
        }
    }

    [Test]
    public async Task Very_wide_fanout_is_offloaded_in_bounded_chunk_jobs()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        harness.FanoutChunkSize = 2;
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new WallabyDocument { ["name"] = p.Name });
        harness.DependsOn<Product, Category?>(p => p.Category);

        // Seed before self-config so only the batch rename below streams.
        var catIds = new List<int>();
        for (var i = 0; i < 5; i++)
        {
            var id = await harness.Db.AddCategoryAsync($"C{i}");
            await harness.Db.AddProductAsync(id, $"chunk_p{i}");
            catIds.Add(id);
        }

        await harness.SelfConfigureAsync();
        await harness.ClearFanoutQueueAsync();

        await harness.StartAsync();
        try
        {
            // All five renames in ONE transaction: 5 distinct lookup keys against a chunk size of 2 are
            // offloaded as jobs of 2 + 2 + 1 while the transaction is consumed; no inline page is read.
            await harness.Db.SetCategoryNamesAsync(catIds.Select((id, i) => (id, $"C{i}b")));
            await harness.WaitUntilAsync(async () => await harness.PendingFanoutJobCountAsync() == 3, Timeout);

            // The trigger transaction acknowledges while delivery rides entirely on the queued chunks.
            await harness.WaitUntilAsync(() => harness.LastAcknowledgedLsn > 0, Timeout);
            capture.For("products").ShouldBeEmpty();

            (await harness.DrainFanoutAsync()).ShouldBe(3);
            await harness.WaitUntilAsync(
                () => capture.For("products").Select(r => r.DocumentId).Distinct().Count() >= 5, Timeout);
        }
        finally
        {
            await harness.StopAsync();
        }

        capture.For("products").Select(r => r.DocumentId).Distinct().Count().ShouldBe(5);
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
        due.ShouldNotBeNull();
        due!.Status.ShouldBe(BackfillStatus.Requested);

        // Marking in progress with a cursor makes it a resumable in-progress job.
        await store.MarkInProgressAsync(
            due.TableQualified, due.LookupHash, KeysetCodec.SerializeCursor([42], ["id"]), CancellationToken.None);
        var resumed = await store.GetNextDueAsync(CancellationToken.None);
        resumed!.Status.ShouldBe(BackfillStatus.InProgress);
        resumed.CursorJson.ShouldNotBeNull();

        // Completing it (guarded on InProgress) deletes the row.
        await store.CompleteAsync(due.TableQualified, due.LookupHash, CancellationToken.None);
        (await store.GetNextDueAsync(CancellationToken.None)).ShouldBeNull();
        (await store.ListAsync(CancellationToken.None)).ShouldBeEmpty();

        // A repeat trigger after completion enqueues a single fresh row.
        await store.EnqueueAsync(spec, CancellationToken.None);
        var rearmed = await store.GetNextDueAsync(CancellationToken.None);
        rearmed!.Status.ShouldBe(BackfillStatus.Requested);
        (await store.ListAsync(CancellationToken.None)).Count(j => j.LookupHash == due.LookupHash)
            .ShouldBe(1);
    }

    [Test]
    public async Task Completing_a_re_armed_job_leaves_the_requested_row()
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
        var job = (await store.GetNextDueAsync(CancellationToken.None))!;
        await store.MarkInProgressAsync(job.TableQualified, job.LookupHash, null, CancellationToken.None);

        // A trigger fires while the job is running: the row re-arms to Requested.
        await store.EnqueueAsync(spec, CancellationToken.None);

        // The finished run must not delete the re-armed request.
        await store.CompleteAsync(job.TableQualified, job.LookupHash, CancellationToken.None);

        var remaining = await store.ListAsync(CancellationToken.None);
        remaining.Count.ShouldBe(1);
        remaining[0].Status.ShouldBe(BackfillStatus.Requested);
    }
}
