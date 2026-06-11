using EFCore.CDC.TestModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Wallaby;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Hosting;

namespace EFCore.CDC.UnitTests;

/// <summary>
/// Registration-level coverage of the two AddWallaby overloads and the CdcOptions options-pipeline
/// integration. Everything here runs offline: building the EF model and creating an NpgsqlDataSource
/// never open a connection.
/// </summary>
public class AddWallabyRegistrationTests
{
    private const string ConnectionString = "Host=localhost;Database=db;Username=u;Password=p";

    private static void AddCaptureConfig(CdcBuilder cdc, string connectionString)
    {
        cdc.UseContext<AppDbContext>()
           .UseConnectionString(connectionString)
           .AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success))
           .Map<Product>()
               .ToSink("sink")
               .UsingTransform((_, changes, _) =>
               {
                   var docs = new Dictionary<DocumentKey, CdcDocument?>();
                   foreach (var c in changes)
                   {
                       docs[c.Key] = new CdcDocument { ["name"] = c.Entity!.Name };
                   }
                   return Task.FromResult<IReadOnlyDictionary<DocumentKey, CdcDocument?>>(docs);
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

        await Assert.That(provider.GetRequiredService<CdcOptions>().ConnectionString).IsEqualTo(ConnectionString);
    }

    [Test]
    public async Task Options_pipeline_composes_in_registration_order()
    {
        var services = NewServices();
        // Before AddWallaby: the builder overrides these.
        services.Configure<CdcOptions>(o => { o.SlotName = "before"; o.MaxBatchSize = 123; });
        services.AddWallaby(cdc =>
        {
            AddCaptureConfig(cdc, ConnectionString);
            cdc.ConfigureOptions(o => { o.SlotName = "builder_slot"; o.MaxBatchSize = 456; });
        });
        // After AddWallaby: overrides the builder. PostConfigure always runs last.
        services.Configure<CdcOptions>(o => o.SlotName = "after");
        services.PostConfigure<CdcOptions>(o => o.PublicationName = "post_pub");

        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<CdcOptions>();

        await Assert.That(options.SlotName).IsEqualTo("after");
        await Assert.That(options.MaxBatchSize).IsEqualTo(456); // builder overrode the earlier Configure
        await Assert.That(options.PublicationName).IsEqualTo("post_pub");
        await Assert.That(options.ChunkSize).IsEqualTo(500); // untouched default
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
        services.Configure<CdcOptions>(configuration.GetSection("Wallaby")); // after AddWallaby → overrides the builder

        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<CdcOptions>();

        await Assert.That(options.ChunkSize).IsEqualTo(42);
        await Assert.That(options.ConnectionString).IsEqualTo(boundConnectionString);
    }

    [Test]
    public async Task IOptions_and_the_plain_singleton_are_the_same_instance()
    {
        var services = NewServices();
        services.AddWallaby(cdc => AddCaptureConfig(cdc, ConnectionString));

        await using var provider = services.BuildServiceProvider();

        var viaOptions = provider.GetRequiredService<IOptions<CdcOptions>>().Value;
        var plain = provider.GetRequiredService<CdcOptions>();
        await Assert.That(ReferenceEquals(viaOptions, plain)).IsTrue();
    }

    [Test]
    public async Task Missing_connection_string_fails_on_first_options_resolution()
    {
        var services = NewServices();
        // No UseConnectionString — a later Configure/binding could still supply it, so registration
        // succeeds and the absence is a validation failure at first resolution.
        services.AddWallaby(cdc => cdc
            .UseContext<AppDbContext>()
            .AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success)));

        await using var provider = services.BuildServiceProvider();

        await Assert.That(() => provider.GetRequiredService<CdcOptions>())
            .Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task Connection_string_can_be_supplied_through_the_options_pipeline_alone()
    {
        var services = NewServices();
        services.AddWallaby(cdc => cdc
            .UseContext<AppDbContext>()
            .AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success))); // no UseConnectionString
        services.PostConfigure<CdcOptions>(o => o.ConnectionString = ConnectionString);

        await using var provider = services.BuildServiceProvider();

        await Assert.That(provider.GetRequiredService<CdcOptions>().ConnectionString).IsEqualTo(ConnectionString);
    }

    [Test]
    public async Task Deferred_overload_surfaces_builder_errors_at_first_resolution()
    {
        var services = NewServices();
        services.AddWallaby((_, cdc) => cdc
            .UseContext<AppDbContext>()
            .UseConnectionString(ConnectionString)
            .AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success))
            .Map<Product>()); // structurally invalid: no .ToSink(...)

        await using var provider = services.BuildServiceProvider(); // registration itself is fine

        await Assert.That(() => provider.GetRequiredService<CdcConfiguration>())
            .Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task Invalid_option_values_fail_on_first_options_resolution()
    {
        var services = NewServices();
        services.AddWallaby(cdc => AddCaptureConfig(cdc, ConnectionString));
        services.PostConfigure<CdcOptions>(o => o.ChunkSize = 0);

        await using var provider = services.BuildServiceProvider();

        await Assert.That(() => provider.GetRequiredService<CdcOptions>())
            .Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task Capture_config_dispatches_the_streaming_hosted_service()
    {
        var services = NewServices();
        services.AddWallaby(cdc => AddCaptureConfig(cdc, ConnectionString));

        await using var provider = services.BuildServiceProvider();

        await Assert.That(provider.GetRequiredService<IHostedService>()).IsTypeOf<CdcBackgroundService>();
    }

    [Test]
    public async Task Provision_only_config_dispatches_the_provisioning_hosted_service()
    {
        var services = NewServices();
        services.AddWallaby(cdc => cdc.UseConnectionString(ConnectionString)); // no sink/mapping

        await using var provider = services.BuildServiceProvider();

        await Assert.That(provider.GetRequiredService<IHostedService>()).IsTypeOf<ExternalSlotProvisioningService>();
    }

    [Test]
    public async Task Provision_only_backfill_manager_resolution_explains_itself()
    {
        var services = NewServices();
        services.AddWallaby(cdc => cdc.UseConnectionString(ConnectionString));

        await using var provider = services.BuildServiceProvider();

        await Assert.That(() => provider.GetRequiredService<ICdcBackfillManager>())
            .Throws<CdcConfigurationException>();
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
        _ = provider.GetRequiredService<CdcConfiguration>();
        _ = provider.GetRequiredService<CdcOptions>();
        _ = provider.GetRequiredService<ICdcStatus>();
        _ = provider.GetRequiredService<IHostedService>();

        await Assert.That(calls).IsEqualTo(1);
    }
}
