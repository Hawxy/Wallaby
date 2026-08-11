using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Client;
using Wallaby.DependencyInjection;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Publication widening end to end against a live streaming node: a schema migration refused over the
/// publication column list runs after <c>WidenPublicationsAsync</c> (the host applies the widen by
/// bouncing its leader session — no slot drop, no re-backfill), and <c>RestorePublicationsAsync</c>
/// re-narrows on the next term with capture flowing throughout.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class RemoteWidenTests(TestModelPostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    [Test]
    public async Task Widen_unblocks_a_migration_without_rebackfill_and_restore_renarrows()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            var capture = new CaptureSink();
            await using var node = await WallabyTestNode.StartAsync(BuildServices(names, capture));
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            var categoryId = await Db.AddCategoryAsync();
            var firstId = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
            await capture.WaitForDocumentsAsync([firstId.ToString()]);

            // The deliberate Description exclusion narrowed products, so the column list refuses the
            // type migration — the situation this feature exists for.
            (await ProductsNarrowedAsync(names.Publication)).ShouldBeTrue();
            var blocked = await Should.ThrowAsync<PostgresException>(
                () => ExecAsync("""ALTER TABLE public.products ALTER COLUMN "Name" TYPE varchar(200)"""));
            blocked.Message.ShouldContain("publication");
            var backfillBefore = await ReadProductsBackfillStampAsync();

            var state = await client.WidenPublicationsAsync(new WallabyWidenOptions
            {
                Timeout = TimeSpan.FromSeconds(60),
            });

            // The running host applied the widen (within the grace period, so not the client fallback)
            // by bouncing its leader session; the migration now runs.
            state.PublicationsWidened.ShouldBeTrue();
            (await ProductsNarrowedAsync(names.Publication)).ShouldBeFalse();
            await ExecAsync("""ALTER TABLE public.products ALTER COLUMN "Name" TYPE varchar(200)""");

            // Capture is still flowing across the bounce: a change committed after the widen arrives
            // via the live stream.
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            var afterId = await Db.AddProductAsync(categoryId, $"after_{names.Suffix}");
            await capture.WaitForDocumentsAsync([afterId.ToString()]);

            // The node surfaces the widened flag (health checks expose it from the same snapshot).
            await Polling.UntilAsync(
                () => node.Services.GetRequiredService<IWallabyStatus>().Current.PublicationsWidened);

            await client.RestorePublicationsAsync();

            // Nothing blocks on the restore; the next leader term re-narrows from the captured model.
            await Polling.UntilAsync(async () => await ProductsNarrowedAsync(names.Publication));
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            var finalId = await Db.AddProductAsync(categoryId, $"final_{names.Suffix}");
            await capture.WaitForDocumentsAsync([finalId.ToString()]);
            await Polling.UntilAsync(
                () => !node.Services.GetRequiredService<IWallabyStatus>().Current.PublicationsWidened);

            // The whole cycle ran on the original slot: the backfill row was never rewritten — no
            // slot-loss gap, no re-backfill (suspension's cost, which widening exists to avoid).
            (await ReadProductsBackfillStampAsync()).ShouldBe(backfillBefore);
            node.Services.GetRequiredService<IWallabyStatus>().Current.Faulted.ShouldBeFalse();
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    private ServiceCollection BuildServices(WallabyNames names, CaptureSink capture)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc => cdc
            .UseEntityFrameworkCore<AppDbContext>()
            .UseConnectionString(pg.ConnectionString)
            .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
            .WithMappings(sink => sink
                .Map<Product>()
                .ToDestination("products")
                .UsingTransform(TestTransforms.ProductNames)
                // The narrowing under test: keeps Description off the wire, putting a column list on
                // public.products.
                .ConsumesAllExcept(p => p.Description)));
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
            o.Advanced.ControlPollInterval = TimeSpan.FromMilliseconds(500);
        });
        services.ReplaceWallabySink("capture", capture);
        return services;
    }

    private async Task ExecAsync(string sql)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<bool> ProductsNarrowedAsync(string publication)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            """
            SELECT count(*) FROM pg_publication p
            JOIN pg_publication_rel pr ON pr.prpubid = p.oid
            JOIN pg_class c ON c.oid = pr.prrelid
            WHERE p.pubname = @p AND c.relname = 'products' AND pr.prattrs IS NOT NULL
            """, conn);
        cmd.Parameters.AddWithValue("p", publication);
        return (long)(await cmd.ExecuteScalarAsync())! > 0;
    }

    // The backfill row is only ever rewritten by a (re)backfill; an unchanged stamp across the widen
    // cycle proves no slot-loss repair fired.
    private async Task<DateTime?> ReadProductsBackfillStampAsync()
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT updated_at FROM wallaby.backfill_state WHERE table_qualified = 'public.products'", conn);
        return await cmd.ExecuteScalarAsync() as DateTime?;
    }

    private async Task ResetControlAsync()
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        try
        {
            await using var cmd = new NpgsqlCommand("DELETE FROM wallaby.control", conn);
            await cmd.ExecuteNonQueryAsync();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
        }
    }
}
