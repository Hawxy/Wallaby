using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Internal;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.IntegrationTests;

/// <summary>
/// Proves slot-loss gap detection end to end: a change committed while the replication slot did not exist
/// can never be streamed, so the next leadership session must detect the checkpoint gap and recover it by
/// re-backfilling the mapped tables.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class SlotLossRecoveryTests(PostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    [Test]
    public async Task Changes_missed_while_the_slot_was_gone_are_rebackfilled()
    {
        var names = WallabyNames.Unique();

        // Node 1: stream one product so a checkpoint exists.
        var firstCapture = new CaptureSink();
        int categoryId, firstId;
        await using (var provider = BuildNode(names, firstCapture))
        {
            await StartAsync(provider);
            try
            {
                await WallabyReadiness.WaitForStreamingAsync(provider);
                categoryId = await Db.AddCategoryAsync();
                firstId = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
                await firstCapture.WaitForDocumentsAsync([firstId.ToString()]);
            }
            finally
            {
                await StopAsync(provider);
            }
        }

        // Destroy the slot and commit a change while none exists — without repair it would never stream.
        await DropSlotAsync(names.Slot);
        var missedId = await Db.AddProductAsync(categoryId, $"missed_{names.Suffix}");

        // Node 2: leadership recreates the slot, detects the checkpoint gap, and re-backfills.
        var secondCapture = new CaptureSink();
        await using (var provider = BuildNode(names, secondCapture))
        {
            await StartAsync(provider);
            try
            {
                await secondCapture.WaitForDocumentsAsync([firstId.ToString(), missedId.ToString()]);

                var latest = secondCapture.LatestByDocumentId(destination: "products");
                latest[missedId.ToString()].Document!["name"].ShouldBe($"missed_{names.Suffix}");
                provider.GetRequiredService<IWallabyStatus>().Current.Faulted.ShouldBeFalse();
            }
            finally
            {
                await StopAsync(provider);
            }
        }
    }

    private ServiceProvider BuildNode(WallabyNames names, CaptureSink capture)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseContext<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .Map<Product>()
                   .ToSink("capture", destination: "products")
                   .UsingTransform((_, changes, _) =>
                   {
                       var docs = new Dictionary<DocumentKey, WallabyDocument?>();
                       foreach (var c in changes)
                       {
                           docs[c.Key] = new WallabyDocument { ["name"] = c.Entity!.Name };
                       }
                       return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(docs);
                   });
        });
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.StandbyRetryInterval = TimeSpan.FromSeconds(1);
        });
        services.ReplaceWallabySink("capture", capture);
        return services.BuildServiceProvider();
    }

    private static async Task StartAsync(ServiceProvider provider)
    {
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StartAsync(CancellationToken.None);
        }
    }

    private static async Task StopAsync(ServiceProvider provider)
    {
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StopAsync(CancellationToken.None);
        }
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
