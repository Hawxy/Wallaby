using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure.Tables;
using Wallaby.Testing;
using Wallaby.TestModel;
using Order = Wallaby.TestInfrastructure.Tables.Order;

namespace Wallaby.Providers.Tables.Tests.Integration;

/// <summary>
/// Production-style <c>AddWallaby</c> registration: transforms query through the container's data
/// source or Wallaby's own, and the provider shares a slot with EF Core.
/// </summary>
[NotInParallel]
[ClassDataSource<TablesFixture>(Shared = SharedType.PerTestSession)]
public class TablesDiTests(TablesFixture pg)
{
    private static async Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> CountLinesAsync(
        NpgsqlDataSource dataSource, IReadOnlyList<ChangeEvent<Order>> changes, CancellationToken ct)
    {
        var documents = new Dictionary<DocumentKey, WallabyDocument?>();
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        foreach (var change in changes)
        {
            await using var cmd = new NpgsqlCommand(
                $"SELECT count(*) FROM {TablesFixture.Schema}.order_lines WHERE order_id = @id", connection);
            cmd.Parameters.AddWithValue("id", change.Entity!.Id);
            documents[change.Key] = new WallabyDocument
            {
                ["name"] = change.Entity.CustomerRef,
                ["lines"] = (long)(await cmd.ExecuteScalarAsync(ct))!,
                ["host"] = connection.Host,
            };
        }
        return documents;
    }

    private static void ConfigureOptions(IServiceCollection services, ReplicationScope names)
        => services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
        });

    private async Task<int> InsertOrderWithLinesAsync(string customer, int lines)
    {
        await using var connection = await pg.DataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var insert = new NpgsqlCommand(
            $"INSERT INTO {TablesFixture.Schema}.orders (customer_ref, total, status) VALUES (@c, 1, 'Paid') RETURNING id", connection, transaction);
        insert.Parameters.AddWithValue("c", customer);
        var id = (int)(await insert.ExecuteScalarAsync())!;
        for (var line = 1; line <= lines; line++)
        {
            await using var cmd = new NpgsqlCommand(
                $"INSERT INTO {TablesFixture.Schema}.order_lines VALUES (@o, @l, 'sku', 1)", connection, transaction);
            cmd.Parameters.AddWithValue("o", id);
            cmd.Parameters.AddWithValue("l", line);
            await cmd.ExecuteNonQueryAsync();
        }
        await transaction.CommitAsync();
        return id;
    }

    [Test]
    public async Task Transforms_query_through_the_registered_data_source()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var capture = new CaptureSink();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(pg.DataSource);
        services.AddWallaby(cdc =>
        {
            cdc.UseTables(TablesFixture.Configure)
               .UseConnectionString(pg.ConnectionString)
               .AddDelegateSink("sink", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .WithMappings(sink => sink.Map<Order>().ToDestination("orders").UsingTransform(CountLinesAsync));
        });
        ConfigureOptions(services, names);
        services.ReplaceWallabySink("sink", capture);

        await using var node = await WallabyTestNode.StartAsync(services);
        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        var id = await InsertOrderWithLinesAsync($"di_{names.Suffix}", lines: 2);
        await capture.WaitForDocumentsAsync([id.ToString()]);

        var document = capture.LatestByDocumentId(destination: "orders")[id.ToString()].Document!;
        document["name"].ShouldBe($"di_{names.Suffix}");
        document["lines"].ShouldBe(2L);
    }

    [Test]
    public async Task Transforms_fall_back_to_wallabys_data_source()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var capture = new CaptureSink();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddWallaby(cdc =>
        {
            cdc.UseTables(TablesFixture.Configure)
               .UseConnectionString(pg.ConnectionString)
               .AddDelegateSink("sink", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .WithMappings(sink => sink.Map<Order>().ToDestination("orders").UsingTransform(CountLinesAsync));
        });
        ConfigureOptions(services, names);
        services.ReplaceWallabySink("sink", capture);

        await using var node = await WallabyTestNode.StartAsync(services);
        node.Services.GetService<NpgsqlDataSource>().ShouldBeNull();
        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        var id = await InsertOrderWithLinesAsync($"fallback_{names.Suffix}", lines: 1);
        await capture.WaitForDocumentsAsync([id.ToString()]);

        var document = capture.LatestByDocumentId(destination: "orders")[id.ToString()].Document!;
        document["lines"].ShouldBe(1L);
        document["host"].ShouldBe(new NpgsqlConnectionStringBuilder(pg.ConnectionString).Host);
    }

    [Test]
    public async Task Shares_a_slot_with_the_ef_core_provider()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var capture = new CaptureSink();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseTables(TablesFixture.Configure)
               .UseConnectionString(pg.ConnectionString)
               .AddDelegateSink("sink", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .WithMappings(sink =>
               {
                   sink.Map<Product>().ToDestination("products").UsingTransform(TestTransforms.ProductNames);
                   sink.Map<Gizmo>().ToDestination("gizmos").UsingTransform((_, changes, _) =>
                       Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(changes.ToDictionary(
                           c => c.Key, c => (WallabyDocument?)new WallabyDocument { ["name"] = c.Entity!.DisplayName })));
               });
        });
        ConfigureOptions(services, names);
        services.ReplaceWallabySink("sink", capture);

        await using var node = await WallabyTestNode.StartAsync(services);
        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        var db = new TestDatabase(pg.ConnectionString);
        var productId = await db.AddProductAsync(await db.AddCategoryAsync(), $"mixed_{names.Suffix}");
        var gizmoId = Guid.NewGuid();
        await using (var cmd = pg.DataSource.CreateCommand($"INSERT INTO {TablesFixture.Schema}.gizmo VALUES (@id, @n, 1)"))
        {
            cmd.Parameters.AddWithValue("id", gizmoId);
            cmd.Parameters.AddWithValue("n", $"gizmo_{names.Suffix}");
            await cmd.ExecuteNonQueryAsync();
        }
        await capture.WaitForDocumentsAsync([productId.ToString(), gizmoId.ToString()]);

        capture.LatestByDocumentId(destination: "products")[productId.ToString()].Document!["name"].ShouldBe($"mixed_{names.Suffix}");
        capture.LatestByDocumentId(destination: "gizmos")[gizmoId.ToString()].Document!["name"].ShouldBe($"gizmo_{names.Suffix}");
    }
}
