using Wallaby.Abstractions;
using Wallaby.Internal.Backfill;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.IntegrationTests;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class BackfillFailureTests(TestModelPostgresFixture pg)
{
    // A backfill chunk whose transform throws (e.g. an enrichment query timing out) must NOT advance the
    // checkpoint to Completed: the window's rows never reached the sink, and a Completed@version state makes
    // the scheduler skip the table forever. The failed chunk must leave the state resumable so a later run
    // (once the underlying failure is fixed) finishes it.
    [Test]
    public async Task Failed_backfill_chunk_does_not_mark_completed_and_resumes()
    {
        var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        harness.ChunkSize = 10; // rows < ChunkSize 
        var version = harness.Names.Suffix; // unique => isolates this table's shared backfill_state row

        harness.AddCaptureSink();
        harness.Map<Product>("capture", "products",
            (_, _, _) => throw new InvalidOperationException("enrichment failed"),
            backfill: true, backfillVersion: version);

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductsAsync(categoryId, Enumerable.Range(0, 3).Select(i => $"p{i}").ToArray());

        await harness.SelfConfigureAsync();
        await harness.StartAsync();

        try
        {
            // The failing chunk faults the backfill loop instead of completing the window as applied.
            await Should.ThrowAsync<Exception>(async () => await harness.RunBackfillAsync(version));

            // No rows reached the sink, so the checkpoint must not read as done.
            var state = (await harness.BackfillManager.GetStatusAsync()).Single(s => s.TransformVersion == version);
            state.Status.ShouldBe(BackfillStatus.InProgress);

            // Which means the next scheduler pass resumes the table rather than skipping it.
            BackfillScheduler.DetermineAction(state, version, new BackfillSchedulerOptions())
                .ShouldBe(BackfillAction.Resume);
        }
        finally
        {
            // The throwing transform also faults the pipeline; that fault is expected on teardown.
            try { await harness.DisposeAsync(); }
            catch (InvalidOperationException) { }
        }
    }
}
