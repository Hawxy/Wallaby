using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Testing;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Internal.Backfill;
using Wallaby.Testing;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// The opt-in watermark visibility fence: on a live server the pg_xact_status predicate passes promptly
/// (a long-running open transaction is in progress, not committed, so it cannot pin the fence), and a
/// fence that can never pass times out with a warning and reads the chunk anyway.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class VisibilityFenceTests(TestModelPostgresFixture pg)
{
    private WallabyTestHarness NewHarness(out string version, out CaptureSink capture)
    {
        var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        version = harness.Names.Suffix; // unique => isolates the shared backfill_state row for products
        capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", "products",
            p => new WallabyDocument { ["name"] = p.Name }, backfill: true, backfillVersion: version);
        return harness;
    }

    [Test]
    public async Task A_fenced_backfill_delivers_every_row()
    {
        await using var harness = NewHarness(out var version, out var capture);
        harness.VisibilityFence = VisibilityFence.FromTimeout(TimeSpan.FromSeconds(5), NullLogger.Instance);

        var categoryId = await harness.Db.AddCategoryAsync();
        var seeded = await harness.Db.AddProductsAsync(categoryId, "fence_a", "fence_b", "fence_c");

        await harness.SelfConfigureAsync();
        await harness.StartAsync();
        await harness.RunBackfillAsync(version);

        var names = capture.For("products").Select(r => r.Document?["name"] as string).ToList();
        foreach (var (_, name) in seeded)
        {
            names.ShouldContain(name);
        }
    }

    [Test]
    public async Task An_open_write_transaction_does_not_stall_the_fence()
    {
        await using var harness = NewHarness(out var version, out var capture);
        harness.VisibilityFence = VisibilityFence.FromTimeout(TimeSpan.FromSeconds(60), NullLogger.Instance);

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductsAsync(categoryId, "open_a", "open_b");

        await harness.SelfConfigureAsync();
        await harness.StartAsync();

        // A transaction that holds an xid and stays open across the whole backfill: it sits in every
        // snapshot's xip list as "in progress", which must not pin the fence.
        await using var writer = new NpgsqlConnection(pg.ConnectionString);
        await writer.OpenAsync();
        await using var txn = await writer.BeginTransactionAsync();
        await using (var cmd = new NpgsqlCommand("SELECT pg_current_xact_id()", writer, txn))
        {
            await cmd.ExecuteScalarAsync();
        }

        var elapsed = Stopwatch.StartNew();
        await harness.RunBackfillAsync(version);
        elapsed.Stop();

        await txn.RollbackAsync();

        // A pinned fence would burn the full 60s timeout per chunk; a passing one is near-instant.
        elapsed.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(30));
        capture.For("products").Count(r => r.Document is not null).ShouldBeGreaterThanOrEqualTo(2);
    }

    [Test]
    public async Task A_timed_out_fence_warns_and_reads_the_chunk_anyway()
    {
        var log = new FakeLogCollector();
        await using var harness = NewHarness(out var version, out var capture);
        harness.VisibilityFence = new VisibilityFence(
            TimeSpan.FromMilliseconds(250), new FakeLogger(log),
            (_, _) => Task.FromResult(false)); // a snapshot that never comes clean

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductsAsync(categoryId, "timeout_a", "timeout_b");

        await harness.SelfConfigureAsync();
        await harness.StartAsync();
        await harness.RunBackfillAsync(version);

        capture.For("products").Count(r => r.Document is not null).ShouldBeGreaterThanOrEqualTo(2);
        log.GetSnapshot().ShouldContain(r =>
            r.Level == LogLevel.Warning
            && r.Message.Contains("Visibility fence")
            && r.Message.Contains("public.products"));
    }
}
