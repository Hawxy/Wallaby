using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// A password supplied through <c>UsePasswordProvider</c> reaches every connection Wallaby opens: the
/// pool (lock, state, readiness probes) and the replication connection.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class PasswordProviderTests(TestModelPostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    private (string ConnectionString, string Password) WithoutPassword()
    {
        var builder = new NpgsqlConnectionStringBuilder(pg.ConnectionString);
        var password = builder.Password!;
        builder.Remove("Password");
        return (builder.ConnectionString, password);
    }

    private ServiceCollection Services(
        ReplicationScope names, string connectionString, Func<CancellationToken, ValueTask<string>> provider,
        CaptureSink capture, FakeLogCollector? log = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(b =>
        {
            if (log is not null)
            {
                b.AddProvider(new FakeLoggerProvider(log));
            }
        });
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(connectionString)
               .UsePasswordProvider(provider)
               .AddDelegateSink("sink", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
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
            o.Advanced.LeaderRetryInterval = TimeSpan.FromMilliseconds(200);
        });
        services.ReplaceWallabySink("sink", capture);
        return services;
    }

    [Test]
    public async Task A_provided_password_streams_end_to_end()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var (connectionString, password) = WithoutPassword();
        var capture = new CaptureSink();
        var calls = 0;

        var services = Services(names, connectionString, _ => { Interlocked.Increment(ref calls); return new ValueTask<string>(password); }, capture);
        await using var node = await WallabyTestNode.StartAsync(services);
        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        var categoryId = await Db.AddCategoryAsync();
        var id = await Db.AddProductAsync(categoryId, $"token_{names.Suffix}");
        await capture.WaitForDocumentsAsync([id.ToString()]);

        capture.LatestByDocumentId(destination: "products")[id.ToString()].Document!["name"].ShouldBe($"token_{names.Suffix}");
        // The pool's periodic provider and the replication connection each fetched the token.
        calls.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Test]
    public async Task A_wrong_provided_password_keeps_the_node_off_leadership()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var (connectionString, _) = WithoutPassword();
        var log = new FakeLogCollector();

        var services = Services(names, connectionString, _ => new ValueTask<string>("not-the-password"), new CaptureSink(), log);
        await using var node = await WallabyTestNode.StartAsync(services);
        var status = node.Services.GetRequiredService<IWallabyStatus>();

        // Connectivity failures are logged and retried rather than recorded as leader failures, so the
        // rejection shows up in the log while the node never becomes leader.
        await Polling.UntilAsync(
            () => log.GetSnapshot().Any(r => r.Exception?.Message.Contains("password authentication failed") == true),
            TimeSpan.FromSeconds(30));

        status.Current.Role.ShouldNotBe(WallabyNodeRole.Leader);
    }
}
