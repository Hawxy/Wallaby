using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.Internal;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Proves slot-loss gap detection end to end: a change committed while the replication slot did not exist
/// can never be streamed, so the next leadership session must detect the checkpoint gap and recover it by
/// re-backfilling the mapped tables.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class SlotLossRecoveryTests(TestModelPostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    [Test]
    public async Task Changes_missed_while_the_slot_was_gone_are_rebackfilled()
    {
        // Scope disposal tolerates the slot already being gone from the mid-test drop below.
        await using var names = ReplicationScope.Unique(pg.ConnectionString);

        // Node 1: stream one product so a checkpoint exists.
        var firstCapture = new CaptureSink();
        int categoryId, firstId;
        await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, firstCapture)))
        {
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            categoryId = await Db.AddCategoryAsync();
            firstId = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
            await firstCapture.WaitForDocumentsAsync([firstId.ToString()]);
        }

        // Destroy the slot and commit a change while none exists — without repair it would never stream.
        await DropSlotAsync(names.Slot);
        var missedId = await Db.AddProductAsync(categoryId, $"missed_{names.Suffix}");

        // Node 2: leadership recreates the slot, detects the checkpoint gap, and re-backfills.
        var secondCapture = new CaptureSink();
        await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, secondCapture)))
        {
            await secondCapture.WaitForDocumentsAsync([firstId.ToString(), missedId.ToString()]);

            var latest = secondCapture.LatestByDocumentId(destination: "products");
            latest[missedId.ToString()].Document!["name"].ShouldBe($"missed_{names.Suffix}");
            node.Services.GetRequiredService<IWallabyStatus>().Current.Faulted.ShouldBeFalse();
        }
    }

    private ServiceCollection BuildServices(WallabyNames names, CaptureSink capture)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .WithMappings(sink => sink
                   .Map<Product>()
                   .ToDestination("products")
                   .UsingTransform(TestTransforms.ProductNames));
        });
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
        });
        services.ReplaceWallabySink("capture", capture);
        return services;
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
