using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Wallaby.Abstractions;
using Wallaby.AspNetCore.HealthChecks;
using Wallaby.DependencyInjection;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Proves a permanently rejecting sink drives the leader into a crash-loop that the health check grades
/// Unhealthy (with the halt cause in the status) and that recovery returns the check to Healthy.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class HealthGradingTests(TestModelPostgresFixture pg)
{
    /// <summary>A capture sink that permanently rejects every batch while armed.</summary>
    private sealed class ToggleRejectSink : ISink
    {
        private readonly CaptureSink _inner = new();
        private volatile bool _armed;

        public string Name => _inner.Name;

        public void Arm() => _armed = true;

        public void Release() => _armed = false;

        public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
            => _armed
                ? Task.FromResult(DeliveryResult.Permanent("document rejected"))
                : _inner.DeliverAsync(batch, ct);

        public Task WaitForDocumentsAsync(IReadOnlyList<string> ids) => _inner.WaitForDocumentsAsync(ids);
    }

    [Test]
    public async Task Crash_looping_leader_goes_unhealthy_and_recovers()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var sink = new ToggleRejectSink();

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
        await using var node = await WallabyTestNode.StartAsync(services);
        await WallabyReadiness.WaitForStreamingAsync(node.Services);
        var status = node.Services.GetRequiredService<IWallabyStatus>();
        var check = new WallabyHealthCheck(status);

        sink.Arm();
        var categoryId = await db.AddCategoryAsync();
        var productId = await db.AddProductAsync(categoryId, $"health_{names.Suffix}");

        await WaitUntilAsync(
            () => status.Current.ConsecutiveLeaderFailures >= 3,
            $"ConsecutiveLeaderFailures never reached 3 (last error: {status.Current.LastError ?? "none"})");

        var unhealthy = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);
        unhealthy.Status.ShouldBe(HealthStatus.Unhealthy);
        unhealthy.Description.ShouldNotBeNull();
        unhealthy.Description.ShouldContain("crash-looping");
        status.Current.LastError.ShouldNotBeNull();
        status.Current.LastError.ShouldContain("'capture'");
        status.Current.LastError.ShouldContain("products");

        sink.Release();
        await sink.WaitForDocumentsAsync([productId.ToString()]);

        // The first delivered + acknowledged transaction resets the counter and the grade.
        await WaitUntilAsync(
            () => status.Current.ConsecutiveLeaderFailures == 0,
            "ConsecutiveLeaderFailures did not reset after recovery");
        var healthy = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);
        healthy.Status.ShouldBe(HealthStatus.Healthy);
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
}
