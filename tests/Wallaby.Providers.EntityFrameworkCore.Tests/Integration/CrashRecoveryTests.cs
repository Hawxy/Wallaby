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
/// Proves a faulting sink fails the leader session — recorded in <see cref="IWallabyStatus"/> and retried
/// with backoff — rather than being mistaken for a clean step-down. A <see cref="TaskCanceledException"/>
/// thrown by a sink (e.g. an HTTP timeout) is the treacherous case: it is an
/// <see cref="OperationCanceledException"/> the session must not confuse with its own cancellation.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class CrashRecoveryTests(TestModelPostgresFixture pg)
{
    /// <summary>A capture sink that throws a raw TaskCanceledException while armed.</summary>
    private sealed class ToggleFaultSink : ISink
    {
        private readonly CaptureSink _inner = new();
        private volatile bool _armed;

        public string Name => _inner.Name;

        public void Arm() => _armed = true;

        public void Release() => _armed = false;

        public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
            => _armed
                // No token: simulates a timeout inside the sink, unrelated to the workload's cancellation.
                ? throw new TaskCanceledException("simulated sink timeout")
                : _inner.DeliverAsync(batch, ct);

        public Task WaitForDocumentsAsync(IReadOnlyList<string> ids) => _inner.WaitForDocumentsAsync(ids);
    }

    [Test]
    public async Task A_sink_thrown_cancellation_is_a_recorded_fault_and_the_change_is_redelivered()
    {
        var names = WallabyNames.Unique();
        var sink = new ToggleFaultSink();

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
            o.Advanced.LeaderRetryInterval = TimeSpan.FromMilliseconds(250);
        });
        services.ReplaceWallabySink("capture", sink);

        var db = new TestDatabase(pg.ConnectionString);
        try
        {
            await using var node = await WallabyTestNode.StartAsync(services);
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            var status = node.Services.GetRequiredService<IWallabyStatus>();

            sink.Arm();
            var categoryId = await db.AddCategoryAsync();
            var productId = await db.AddProductAsync(categoryId, $"crash_{names.Suffix}");

            // The sink's TaskCanceledException must surface as a leader-session failure, not a clean step-down.
            await WaitUntilAsync(
                () => status.Current.ConsecutiveLeaderFailures >= 1,
                $"ConsecutiveLeaderFailures stayed 0 (last error: {status.Current.LastError ?? "none"})");

            sink.Release();
            await sink.WaitForDocumentsAsync([productId.ToString()]);
        }
        finally
        {
            await DropSlotAndPublicationAsync(names);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string timeoutMessage)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException(timeoutMessage);
            }
            await Task.Delay(100);
        }
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
