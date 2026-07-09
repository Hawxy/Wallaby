using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Internal;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Proves the sink disposal capability end to end: a sink implementing <see cref="IAsyncDisposable"/>
/// is disposed exactly once when the node shuts down, after streaming has stopped.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class SinkDisposalTests(TestModelPostgresFixture pg)
{
    /// <summary>A capture sink that records when it is disposed.</summary>
    private sealed class DisposableCaptureSink : ISink, IAsyncDisposable
    {
        private readonly CaptureSink _inner = new();

        public int DisposeCount { get; private set; }

        public string Name => _inner.Name;

        public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct) =>
            _inner.DeliverAsync(batch, ct);

        public Task WaitForDocumentsAsync(IReadOnlyList<string> ids) => _inner.WaitForDocumentsAsync(ids);

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            return ValueTask.CompletedTask;
        }
    }

    [Test]
    public async Task Sinks_are_disposed_once_at_node_shutdown()
    {
        var names = WallabyNames.Unique();
        var sink = new DisposableCaptureSink();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .WithMappings(s => s
                   .Map<Product>()
                   .ToDestination("products")
                   .UsingTransform(TestTransforms.ProductNames));
        });
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
        });
        services.ReplaceWallabySink("capture", sink);

        var db = new TestDatabase(pg.ConnectionString);
        try
        {
            await using (var node = await WallabyTestNode.StartAsync(services))
            {
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                var categoryId = await db.AddCategoryAsync();
                var productId = await db.AddProductAsync(categoryId, $"disposal_{names.Suffix}");
                await sink.WaitForDocumentsAsync([productId.ToString()]);

                // Streaming delivers while the sink is live; disposal only happens at shutdown.
                sink.DisposeCount.ShouldBe(0);
            }

            sink.DisposeCount.ShouldBe(1);
        }
        finally
        {
            await DropSlotAndPublicationAsync(names);
        }
    }

    [Test]
    public async Task The_harness_disposes_sinks_it_materialized()
    {
        var sink = new DisposableCaptureSink();
        var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        try
        {
            harness.AddSink(sink)
                .Project<Product>(sink.Name, destination: null, p => new WallabyDocument { ["name"] = p.Name });

            await harness.SelfConfigureAsync();
            await harness.StartAsync();
            await harness.StopAsync();
            sink.DisposeCount.ShouldBe(0);
        }
        finally
        {
            await harness.DisposeAsync();
            await DropSlotAndPublicationAsync(harness.Names);
        }

        sink.DisposeCount.ShouldBe(1);
    }

    // Slots and publications survive the shared session database, so tests drop their own to avoid
    // exhausting max_replication_slots. The prior node's replication connection can linger briefly
    // after shutdown; retry until the server considers the slot inactive.
    private async Task DropSlotAndPublicationAsync(WallabyNames names)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await PgExec.ExecuteAsync(
                    conn,
                    "SELECT pg_drop_replication_slot(@s) WHERE EXISTS " +
                    "(SELECT 1 FROM pg_replication_slots WHERE slot_name = @s)",
                    default,
                    ("s", names.Slot));
                break;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ObjectInUse && attempt < 50)
            {
                await Task.Delay(100);
            }
        }
        await PgExec.ExecuteAsync(conn, $"DROP PUBLICATION IF EXISTS {PgExec.QuoteIdentifier(names.Publication)}", default);
    }
}
