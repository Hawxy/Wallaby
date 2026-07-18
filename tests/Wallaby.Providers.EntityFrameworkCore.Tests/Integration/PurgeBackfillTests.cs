using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Client;
using Wallaby.DependencyInjection;
using Wallaby.Internal;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Purge-then-backfill convergence end to end: a purge empties the sink destination before the fresh
/// backfill's snapshot, so documents whose source rows disappeared without a delivered delete (truncate,
/// deletes inside a slot-loss gap) don't survive the re-backfill.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class PurgeBackfillTests(TestModelPostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    [Test]
    public async Task A_purge_request_converges_the_sink_after_a_truncate()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var client = new WallabyControlClient(pg.ConnectionString);
        var capture = new CaptureSink();

        await using var node = await WallabyTestNode.StartAsync(BuildServices(names, capture));
        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        var categoryId = await Db.AddCategoryAsync();
        var staleId = await Db.AddProductAsync(categoryId, $"stale_{names.Suffix}");
        await capture.WaitForDocumentsAsync([staleId.ToString()]);

        // Truncate is warn-only, so the stale document survives in the sink; the replacement row proves
        // the stream keeps flowing.
        await ExecAsync("TRUNCATE TABLE products CASCADE");
        var keptId = await Db.AddProductAsync(categoryId, $"kept_{names.Suffix}");
        await capture.WaitForDocumentsAsync([keptId.ToString()]);
        capture.LatestByDocumentId("products").ShouldContainKey(staleId.ToString());

        await client.RequestBackfillAsync("public.products", purge: true);
        await capture.WaitForAsync(records =>
            capture.Purges.Count > 0 &&
            records.Any(r => r.DocumentId == keptId.ToString() && r.Metadata.IsBackfill));

        // The purge emptied the destination and the snapshot re-upserted only the surviving row.
        var latest = capture.LatestByDocumentId("products");
        latest.ShouldContainKey(keptId.ToString());
        latest.ShouldNotContainKey(staleId.ToString());
        capture.Purges.ShouldHaveSingleItem().Destination.ShouldBe("products");
    }

    [Test]
    public async Task A_plain_request_leaves_the_stale_document_behind()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var capture = new CaptureSink();

        await using var node = await WallabyTestNode.StartAsync(BuildServices(names, capture));
        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        var categoryId = await Db.AddCategoryAsync();
        var staleId = await Db.AddProductAsync(categoryId, $"stale_{names.Suffix}");
        await capture.WaitForDocumentsAsync([staleId.ToString()]);

        await ExecAsync("TRUNCATE TABLE products CASCADE");
        var keptId = await Db.AddProductAsync(categoryId, $"kept_{names.Suffix}");
        await capture.WaitForDocumentsAsync([keptId.ToString()]);

        await node.Services.GetRequiredService<IWallabyBackfillManager>().RequestBackfillAsync<Product>();
        await capture.WaitForAsync(records =>
            records.Any(r => r.DocumentId == keptId.ToString() && r.Metadata.IsBackfill));

        // Upsert-only: without the purge, the truncated row's document remains — the documented gap.
        capture.LatestByDocumentId("products").ShouldContainKey(staleId.ToString());
        capture.Purges.ShouldBeEmpty();
    }

    [Test]
    public async Task Slot_gap_repair_purges_when_opted_in_so_a_missed_delete_converges()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var capture = new CaptureSink();

        // Node 1: deliver two products so the sink and checkpoint both know them.
        int categoryId, keptId, missedDeleteId;
        await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, capture)))
        {
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            categoryId = await Db.AddCategoryAsync();
            keptId = await Db.AddProductAsync(categoryId, $"kept_{names.Suffix}");
            missedDeleteId = await Db.AddProductAsync(categoryId, $"gone_{names.Suffix}");
            await capture.WaitForDocumentsAsync([keptId.ToString(), missedDeleteId.ToString()]);
        }

        // Delete one row while no slot exists: without a purge that delete can never reach the sink.
        await DropSlotAsync(names.Slot);
        await ExecAsync($"DELETE FROM products WHERE \"Id\" = {missedDeleteId}");

        // Node 2 (same sink, purge opted in): repair purges, and the re-backfill converges.
        await using (var node = await WallabyTestNode.StartAsync(
            BuildServices(names, capture, o => o.PurgeOnSlotGapRepair = true)))
        {
            await capture.WaitForAsync(records =>
                capture.Purges.Count > 0 &&
                records.Any(r => r.DocumentId == keptId.ToString() && r.Metadata.IsBackfill));

            var latest = capture.LatestByDocumentId("products");
            latest.ShouldContainKey(keptId.ToString());
            latest.ShouldNotContainKey(missedDeleteId.ToString());
        }
    }

    [Test]
    public async Task Slot_gap_repair_without_the_option_leaves_the_missed_delete_stale()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var capture = new CaptureSink();

        int categoryId, keptId, missedDeleteId;
        await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, capture)))
        {
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            categoryId = await Db.AddCategoryAsync();
            keptId = await Db.AddProductAsync(categoryId, $"kept_{names.Suffix}");
            missedDeleteId = await Db.AddProductAsync(categoryId, $"gone_{names.Suffix}");
            await capture.WaitForDocumentsAsync([keptId.ToString(), missedDeleteId.ToString()]);
        }

        await DropSlotAsync(names.Slot);
        await ExecAsync($"DELETE FROM products WHERE \"Id\" = {missedDeleteId}");

        await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, capture)))
        {
            await capture.WaitForAsync(records =>
                records.Any(r => r.DocumentId == keptId.ToString() && r.Metadata.IsBackfill));

            // The default repair is upsert-only, so the deleted row's document survives.
            capture.LatestByDocumentId("products").ShouldContainKey(missedDeleteId.ToString());
            capture.Purges.ShouldBeEmpty();
        }
    }

    [Test]
    public async Task A_version_change_with_purge_opted_in_converges_the_sink()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var capture = new CaptureSink();

        // Node 1 (v1): the auto backfill completes at v1 and a product is delivered.
        int categoryId, staleId;
        await using (var node = await WallabyTestNode.StartAsync(
            BuildServices(names, capture, version: "v1", purgeOnVersionChange: false)))
        {
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            categoryId = await Db.AddCategoryAsync();
            staleId = await Db.AddProductAsync(categoryId, $"stale_{names.Suffix}");
            await capture.WaitForDocumentsAsync([staleId.ToString()]);
        }

        // While no node runs, the delivered row is truncated away and replaced.
        await ExecAsync("TRUNCATE TABLE products CASCADE");
        var keptId = await Db.AddProductAsync(categoryId, $"kept_{names.Suffix}");

        // Node 2 (v2, purge opted in): the version-change re-backfill purges first.
        await using (var node = await WallabyTestNode.StartAsync(
            BuildServices(names, capture, version: "v2", purgeOnVersionChange: true)))
        {
            await capture.WaitForAsync(records =>
                capture.Purges.Count > 0 &&
                records.Any(r => r.DocumentId == keptId.ToString() && r.Metadata.IsBackfill));

            var latest = capture.LatestByDocumentId("products");
            latest.ShouldContainKey(keptId.ToString());
            latest.ShouldNotContainKey(staleId.ToString());
        }
    }

    private ServiceCollection BuildServices(
        WallabyNames names,
        CaptureSink capture,
        Action<WallabyOptions>? configure = null,
        string? version = null,
        bool purgeOnVersionChange = false)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc => cdc
            .UseEntityFrameworkCore<AppDbContext>()
            .UseConnectionString(pg.ConnectionString)
            .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
            .WithMappings(sink =>
            {
                var map = sink.Map<Product>()
                    .ToDestination("products")
                    .UsingTransform(TestTransforms.ProductNames);
                if (version is not null)
                {
                    map.WithBackfillVersion(version, purgeOnVersionChange);
                }
            }));
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
            configure?.Invoke(o);
        });
        services.ReplaceWallabySink("capture", capture);
        return services;
    }

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    // The prior node's replication connection can linger briefly after StopAsync; retry until the server
    // considers the slot inactive.
    private async Task DropSlotAsync(string slot)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await PgExec.ExecuteAsync(conn, "SELECT pg_drop_replication_slot(@s)", default, ("s", slot));
                return;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ObjectInUse && attempt < 50)
            {
                await Task.Delay(100);
            }
        }
    }
}
