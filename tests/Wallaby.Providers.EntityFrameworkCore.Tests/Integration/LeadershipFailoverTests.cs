using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Internal.Cluster;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Leadership flapping: two nodes share one slot/publication, and the leader's advisory-lock connection
/// is repeatedly killed. Each cycle exactly one leader must emerge, delivery must resume on it, and a
/// lost lock must remain the clean step-down path — no fault, no lingering failure count.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class LeadershipFailoverTests(TestModelPostgresFixture pg)
{
    [Test]
    public async Task Repeatedly_killing_the_lock_connection_fails_over_and_delivery_resumes()
    {
        var names = WallabyNames.Unique();
        var captureA = new CaptureSink();
        var captureB = new CaptureSink();
        var db = new TestDatabase(pg.ConnectionString);

        try
        {
            await using var nodeA = await WallabyTestNode.StartAsync(BuildServices(names, captureA));
            await using var nodeB = await WallabyTestNode.StartAsync(BuildServices(names, captureB));
            var statusA = nodeA.Services.GetRequiredService<IWallabyStatus>();
            var statusB = nodeB.Services.GetRequiredService<IWallabyStatus>();

            // WallabyReadiness is node-scoped (either node may win the initial election), so gate on the
            // shared slot directly.
            await WaitUntilAsync(
                () => SlotActiveAsync(names.Slot),
                "the initial leader never started streaming");

            var categoryId = await db.AddCategoryAsync();

            for (var cycle = 1; cycle <= 3; cycle++)
            {
                (await TerminateLockHolderAsync(names.Slot)).ShouldBeTrue($"no backend held the lock in cycle {cycle}");

                // Exactly one leader emerges once the loser's step-down and the winner's takeover settle.
                await WaitUntilAsync(
                    () => Task.FromResult(CountLeaders(statusA, statusB) == 1),
                    $"cycle {cycle} did not settle on exactly one leader " +
                    $"(A: {statusA.Current.Role}, B: {statusB.Current.Role})");

                // Delivery resumes on whichever node now leads. The shared slot retains WAL across the
                // handover, so a change written mid-failover must still arrive.
                var id = await db.AddProductAsync(categoryId, $"failover_cycle{cycle}_{names.Suffix}");
                await WaitUntilAsync(
                    () => Task.FromResult(Delivered(captureA, id) || Delivered(captureB, id)),
                    $"the change of cycle {cycle} was never delivered after failover");
            }

            // A lost lock is a clean step-down, and the surviving leader has acknowledged deliveries:
            // neither node may end faulted or with an accumulated failure count.
            statusA.Current.Faulted.ShouldBeFalse();
            statusB.Current.Faulted.ShouldBeFalse();
            await WaitUntilAsync(
                () => Task.FromResult(
                    statusA.Current.ConsecutiveLeaderFailures == 0 && statusB.Current.ConsecutiveLeaderFailures == 0),
                "a node ended with a lingering leader-failure count " +
                $"(A: {statusA.Current.ConsecutiveLeaderFailures}, B: {statusB.Current.ConsecutiveLeaderFailures})");
        }
        finally
        {
            await PostgresReplicationCleanup.DropAsync(pg.ConnectionString, names);
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
            o.Advanced.StandbyRetryInterval = TimeSpan.FromMilliseconds(250);
        });
        services.ReplaceWallabySink("capture", capture);
        return services;
    }

    /// <summary>Kill the backend holding the leadership advisory lock; false when nobody holds it.</summary>
    private async Task<bool> TerminateLockHolderAsync(string slotName)
    {
        // The 64-bit advisory key surfaces in pg_locks split across classid (high) and objid (low).
        var key = unchecked((ulong)PostgresAdvisoryLock.StableKey(slotName));
        await using var connection = await pg.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT count(pg_terminate_backend(pid))
            FROM pg_locks
            WHERE locktype = 'advisory' AND granted AND classid::bigint = @hi AND objid::bigint = @lo
            """,
            connection);
        command.Parameters.AddWithValue("hi", (long)(key >> 32));
        command.Parameters.AddWithValue("lo", (long)(uint)key);
        return (long)(await command.ExecuteScalarAsync())! > 0;
    }

    private async Task<bool> SlotActiveAsync(string slotName)
    {
        await using var command = pg.DataSource.CreateCommand(
            "SELECT active FROM pg_replication_slots WHERE slot_name = $1");
        command.Parameters.AddWithValue(slotName);
        return await command.ExecuteScalarAsync() is true;
    }

    private static int CountLeaders(params IWallabyStatus[] statuses)
        => statuses.Count(s => s.Current.Role == WallabyNodeRole.Leader);

    private static bool Delivered(CaptureSink capture, int productId)
        => capture.LatestByDocumentId(destination: "products").ContainsKey(productId.ToString());

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string timeoutMessage)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException(timeoutMessage);
            }
            await Task.Delay(100);
        }
    }
}
