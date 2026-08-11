using Wallaby.Abstractions;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.State;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class BackfillFailureTests(TestModelPostgresFixture pg)
{
    // A backfill chunk whose transform throws (e.g. an enrichment query timing out) must NOT advance the
    // checkpoint to Completed: the window's rows never reached the sink, and a Completed@version state makes
    // the scheduler skip the table forever. The failure backs off the table alone (the pass itself never
    // faults the leader) and leaves the state resumable so a later run finishes it.
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
            // The failing chunk is recorded against the table (with backoff) instead of faulting the pass.
            var nextRetryAt = await harness.RunBackfillAsync(version);
            nextRetryAt.ShouldNotBeNull();

            // No rows reached the sink, so the checkpoint must not read as done — and the failure ledger
            // carries the cause.
            var state = (await harness.BackfillManager.GetStatusAsync()).Single(s => s.TransformVersion == version);
            state.Status.ShouldBe(BackfillStatus.InProgress);
            state.Attempts.ShouldBe(1);
            state.LastError.ShouldNotBeNull();
            state.NextAttemptAt.ShouldBe(nextRetryAt);

            // A pass inside the backoff window leaves the table alone (no second attempt burned).
            await harness.RunBackfillAsync(version);
            (await harness.BackfillManager.GetStatusAsync()).Single(s => s.TransformVersion == version)
                .Attempts.ShouldBe(1);

            // Once due again, the scheduler resumes the table rather than skipping it.
            BackfillScheduler.Decide(state, version, purgeOnVersionChange: false, new BackfillSchedulerOptions())
                .Action.ShouldBe(BackfillAction.Resume);

            // The shared products row must not stay backed off, or a later test's pass would skip it.
            await new PostgresBackfillStore(pg.DataSource).ClearFailureAsync(
                state.TableQualifiedName, CancellationToken.None);
        }
        finally
        {
            // The throwing transform also faults the pipeline; that fault is expected on teardown.
            try { await harness.DisposeAsync(); }
            catch (InvalidOperationException) { }
        }
    }
}
