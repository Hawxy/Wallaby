using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Client.DependencyInjection;

namespace Wallaby.Client.Tests.Unit;

public class RegistrationTests
{
    private const string ConnectionString = "Host=localhost;Database=wallaby_test";

    [Test]
    public async Task Connection_string_overload_registers_a_singleton_client()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddWallabyControlClient(ConnectionString);

        await using var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<WallabyControlClient>();
        provider.GetRequiredService<WallabyControlClient>().ShouldBeSameAs(client);
    }

    [Test]
    public async Task Data_source_factory_overload_registers_a_singleton_client()
    {
        await using var dataSource = NpgsqlDataSource.Create(ConnectionString);
        var services = new ServiceCollection();
        services.AddWallabyControlClient(_ => dataSource);

        await using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<WallabyControlClient>().ShouldNotBeNull();
    }

    [Test]
    public async Task Container_data_source_overload_resolves_the_registered_data_source()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_ => NpgsqlDataSource.Create(ConnectionString));
        services.AddWallabyControlClient();

        await using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<WallabyControlClient>().ShouldNotBeNull();
    }

    [Test]
    public async Task Registration_is_idempotent()
    {
        var services = new ServiceCollection();
        services.AddWallabyControlClient(ConnectionString);
        services.AddWallabyControlClient("Host=other;Database=ignored");

        await using var provider = services.BuildServiceProvider();
        provider.GetServices<WallabyControlClient>().Count().ShouldBe(1);
    }

    [Test]
    public async Task Works_without_logging_registered()
    {
        var services = new ServiceCollection();
        services.AddWallabyControlClient(ConnectionString);

        await using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<WallabyControlClient>().ShouldNotBeNull();
    }
}
