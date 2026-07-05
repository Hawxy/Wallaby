using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.EntityFrameworkCore;
using Wallaby.Hosting;
using Wallaby.TestModel;

namespace Wallaby.EntityFrameworkCore.UnitTests;

/// <summary>
/// Registration-level coverage of the two AddWallaby overloads and the WallabyOptions options-pipeline
/// integration. Everything here runs offline: building the EF model and creating an NpgsqlDataSource
/// never open a connection.
/// </summary>
public class AddWallabyRegistrationTests
{
    private const string ConnectionString = "Host=localhost;Database=db;Username=u;Password=p";

    private static void AddCaptureConfig(WallabyBuilder cdc, string connectionString)
    {
        cdc.UseEntityFrameworkCore<AppDbContext>()
           .UseConnectionString(connectionString)
           .AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success))
           .Map<Product>()
               .ToSink("sink")
               .UsingTransform((_, changes, _) =>
               {
                   var docs = new Dictionary<DocumentKey, WallabyDocument?>();
                   foreach (var c in changes)
                   {
                       docs[c.Key] = new WallabyDocument { ["name"] = c.Entity!.Name };
                   }
                   return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(docs);
               });
    }

    private static ServiceCollection NewServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(ConnectionString));
        return services;
    }

    [Test]
    public async Task Deferred_configure_reads_services_from_the_provider()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["ConnectionStrings:App"] = ConnectionString })
            .Build();

        var services = NewServices();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddWallaby((sp, cdc) =>
            AddCaptureConfig(cdc, sp.GetRequiredService<IConfiguration>().GetConnectionString("App")!));

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<WallabyOptions>().ConnectionString.ShouldBe(ConnectionString);
    }

    [Test]
    public async Task Options_pipeline_composes_in_registration_order()
    {
        var services = NewServices();
        // Before AddWallaby: the builder overrides these.
        services.Configure<WallabyOptions>(o => { o.SlotName = "before"; o.MaxBatchSize = 123; });
        services.AddWallaby(cdc =>
        {
            AddCaptureConfig(cdc, ConnectionString);
            cdc.ConfigureOptions(o => { o.SlotName = "builder_slot"; o.MaxBatchSize = 456; });
        });
        // After AddWallaby: overrides the builder. PostConfigure always runs last.
        services.Configure<WallabyOptions>(o => o.SlotName = "after");
        services.PostConfigure<WallabyOptions>(o => o.PublicationName = "post_pub");

        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<WallabyOptions>();

        options.SlotName.ShouldBe("after");
        options.MaxBatchSize.ShouldBe(456); // builder overrode the earlier Configure
        options.PublicationName.ShouldBe("post_pub");
        options.ChunkSize.ShouldBe(500); // untouched default
    }

    [Test]
    public async Task Configuration_binding_sets_values_including_the_connection_string()
    {
        const string boundConnectionString = "Host=bound;Database=db;Username=u;Password=p";
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Wallaby:ChunkSize"] = "42",
                ["Wallaby:ConnectionString"] = boundConnectionString,
            })
            .Build();

        var services = NewServices();
        services.AddWallaby(cdc => AddCaptureConfig(cdc, ConnectionString));
        services.Configure<WallabyOptions>(configuration.GetSection("Wallaby")); // after AddWallaby → overrides the builder

        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<WallabyOptions>();

        options.ChunkSize.ShouldBe(42);
        options.ConnectionString.ShouldBe(boundConnectionString);
    }

    [Test]
    public async Task IOptions_and_the_plain_singleton_are_the_same_instance()
    {
        var services = NewServices();
        services.AddWallaby(cdc => AddCaptureConfig(cdc, ConnectionString));

        await using var provider = services.BuildServiceProvider();

        var viaOptions = provider.GetRequiredService<IOptions<WallabyOptions>>().Value;
        var plain = provider.GetRequiredService<WallabyOptions>();
        ReferenceEquals(viaOptions, plain).ShouldBeTrue();
    }

    [Test]
    public async Task Missing_connection_string_fails_on_first_options_resolution()
    {
        var services = NewServices();
        // No UseConnectionString — a later Configure/binding could still supply it, so registration
        // succeeds and the absence is a validation failure at first resolution.
        services.AddWallaby(cdc => cdc
            .UseEntityFrameworkCore<AppDbContext>()
            .AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success)));

        await using var provider = services.BuildServiceProvider();

        Should.Throw<WallabyConfigurationException>(() => provider.GetRequiredService<WallabyOptions>());
    }

    [Test]
    public async Task Connection_string_can_be_supplied_through_the_options_pipeline_alone()
    {
        var services = NewServices();
        services.AddWallaby(cdc => cdc
            .UseEntityFrameworkCore<AppDbContext>()
            .AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success))); // no UseConnectionString
        services.PostConfigure<WallabyOptions>(o => o.ConnectionString = ConnectionString);

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<WallabyOptions>().ConnectionString.ShouldBe(ConnectionString);
    }

    [Test]
    public async Task Deferred_overload_surfaces_builder_errors_at_first_resolution()
    {
        var services = NewServices();
        services.AddWallaby((_, cdc) => cdc
            .UseEntityFrameworkCore<AppDbContext>()
            .UseConnectionString(ConnectionString)
            .AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success))
            .Map<Product>()); // structurally invalid: no .ToSink(...)

        await using var provider = services.BuildServiceProvider(); // registration itself is fine

        Should.Throw<WallabyConfigurationException>(() => provider.GetRequiredService<WallabyConfiguration>());
    }

    [Test]
    public async Task Invalid_option_values_fail_on_first_options_resolution()
    {
        var services = NewServices();
        services.AddWallaby(cdc => AddCaptureConfig(cdc, ConnectionString));
        services.PostConfigure<WallabyOptions>(o => o.ChunkSize = 0);

        await using var provider = services.BuildServiceProvider();

        Should.Throw<WallabyConfigurationException>(() => provider.GetRequiredService<WallabyOptions>());
    }

    [Test]
    public async Task Capture_config_dispatches_the_streaming_hosted_service()
    {
        var services = NewServices();
        services.AddWallaby(cdc => AddCaptureConfig(cdc, ConnectionString));

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IHostedService>().ShouldBeOfType<WallabyBackgroundService>();
    }

    [Test]
    public async Task Provision_only_config_dispatches_the_provisioning_hosted_service()
    {
        var services = NewServices();
        services.AddWallaby(cdc => cdc.UseConnectionString(ConnectionString)); // no sink/mapping

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IHostedService>().ShouldBeOfType<ExternalSlotProvisioningService>();
    }

    [Test]
    public async Task Provision_only_backfill_manager_resolution_explains_itself()
    {
        var services = NewServices();
        services.AddWallaby(cdc => cdc.UseConnectionString(ConnectionString));

        await using var provider = services.BuildServiceProvider();

        Should.Throw<WallabyConfigurationException>(() => provider.GetRequiredService<IWallabyBackfillManager>());
    }

    [Test]
    public async Task Deferred_configure_runs_exactly_once()
    {
        var calls = 0;
        var services = NewServices();
        services.AddWallaby((_, cdc) =>
        {
            calls++;
            AddCaptureConfig(cdc, ConnectionString);
        });

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<WallabyConfiguration>();
        _ = provider.GetRequiredService<WallabyOptions>();
        _ = provider.GetRequiredService<IWallabyStatus>();
        _ = provider.GetRequiredService<IHostedService>();

        calls.ShouldBe(1);
    }
}
