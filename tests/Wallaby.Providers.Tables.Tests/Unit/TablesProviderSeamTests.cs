using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Internal;
using Wallaby.Providers.Tables.Internal;

namespace Wallaby.Providers.Tables.Tests.Unit;

/// <summary>
/// Registration seams: UseTables satisfies the builder's provider requirement, the provider-typed
/// UsingTransform overloads pin mappings, column selections reach the capture spec, and the default
/// session data source resolves from the container.
/// </summary>
public class TablesProviderSeamTests
{
    private const string ConnectionString = "Host=localhost;Database=db;Username=u;Password=p";

    private static (WallabyBuilder Builder, WallabySinkBuilder Sink) CapturingBuilder()
    {
        var builder = new WallabyBuilder(new ServiceCollection());
        builder.UseConnectionString(ConnectionString);
        var sink = builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success));
        return (builder, sink);
    }

    private static Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> Drop(
        NpgsqlDataSource _, IReadOnlyList<ChangeEvent<Order>> changes, CancellationToken ct)
        => Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(
            changes.ToDictionary(c => c.Key, _ => (WallabyDocument?)null));

    [Test]
    public void UseTables_satisfies_the_provider_requirement()
    {
        var (builder, sink) = CapturingBuilder();
        builder.UseTables(t => t.Add<Order>());
        sink.WithMappings(s => s.Map<Order>().UsingTransform(Drop));

        var config = builder.Build();

        var registration = config.Providers.ShouldHaveSingleItem();
        registration.Name.ShouldBe("Tables");
        registration.ModelProvider(null!).Handles(typeof(Order)).ShouldBeTrue();
    }

    [Test]
    public void Registering_tables_twice_fails_fast()
    {
        var (builder, _) = CapturingBuilder();
        builder.UseTables(t => t.Add<Order>());

        Should.Throw<WallabyConfigurationException>(() => builder.UseTables(t => t.Add<Widget>()));
    }

    [Test]
    public void Configuration_errors_surface_at_UseTables()
    {
        var (builder, _) = CapturingBuilder();

        Should.Throw<WallabyConfigurationException>(() => builder.UseTables(t => t.Add<NoKey>()));
    }

    [Test]
    public void UsingTransform_pins_the_mapping_to_the_tables_provider()
    {
        var (builder, sink) = CapturingBuilder();
        builder.UseTables(t => t.Add<Order>());
        sink.WithMappings(s => s.Map<Order>().UsingTransform(Drop));

        builder.Build().AllMappings.Single().ProviderName.ShouldBe("Tables");
    }

    [Test]
    public void Consumes_reaches_the_capture_spec_as_a_column_selection()
    {
        var (builder, sink) = CapturingBuilder();
        builder.UseTables(t => t.Add<Order>());
        sink.WithMappings(s => s.Map<Order>().Consumes(o => o.Total, o => o.Status).UsingTransform(Drop));

        var spec = builder.Build().ToCaptureSpec("Tables", new Dictionary<Type, string> { [typeof(Order)] = "Tables" });

        var selection = spec.DeclaredColumnSelections[typeof(Order)].ShouldHaveSingleItem();
        selection.Mode.ShouldBe(ColumnSelectionMode.Include);
        selection.PropertyNames.ShouldBe(["Total", "Status"]);
    }

    [Test]
    public async Task Transforms_receive_the_container_data_source_when_one_is_registered()
    {
        await using var registered = NpgsqlDataSource.Create(ConnectionString);
        var services = new ServiceCollection();
        services.AddSingleton(registered);
        var builder = new WallabyBuilder(services);
        builder.UseConnectionString(ConnectionString);
        builder.UseTables(t => t.Add<Order>());

        await using var provider = services.BuildServiceProvider();
        var sessions = builder.Build().Providers.Single().EnrichmentSessions(provider);

        sessions.IsScoped.ShouldBeFalse();
        sessions.Lease(null).Session.ShouldBeSameAs(registered);
    }

    [Test]
    public async Task Transforms_fall_back_to_wallabys_own_data_source()
    {
        var services = new ServiceCollection();
        await using var wallaby = new WallabyDataSource(ConnectionString);
        services.AddSingleton(wallaby);
        var builder = new WallabyBuilder(services);
        builder.UseConnectionString(ConnectionString);
        builder.UseTables(t => t.Add<Order>());

        await using var provider = services.BuildServiceProvider();
        var sessions = builder.Build().Providers.Single().EnrichmentSessions(provider);

        sessions.Lease(null).Session.ShouldBeSameAs(wallaby.Source);
    }

    [Test]
    public void An_explicit_data_source_factory_is_used_as_is()
    {
        using var explicitSource = NpgsqlDataSource.Create(ConnectionString);
        var (builder, sink) = CapturingBuilder();
        builder.UseTables(_ => explicitSource, t => t.Add<Order>());
        sink.WithMappings(s => s.Map<Order>().UsingTransform(Drop));

        var sessions = builder.Build().Providers.Single().EnrichmentSessions(null!);

        sessions.Lease(null).Session.ShouldBeSameAs(explicitSource);
        sessions.ShouldBeOfType<TablesEnrichmentSessionProvider>();
    }
}
