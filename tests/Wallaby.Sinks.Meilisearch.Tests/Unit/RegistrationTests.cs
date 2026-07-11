using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using static Wallaby.Sinks.Meilisearch.Tests.Unit.MeilisearchTestHelpers;

namespace Wallaby.Sinks.Meilisearch.Tests.Unit;

/// <summary>Validation and container requirements of <c>AddMeilisearchSink</c>.</summary>
public class RegistrationTests
{
    private static readonly MeilisearchSinkOptions ValidOptions = new() { Host = "http://localhost:7700" };

    [Test]
    public void Sink_resolution_requires_the_http_client_factory()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();

        var ex = Should.Throw<WallabyConfigurationException>(
            () => MeilisearchBuilderExtensions.CreateSink("meili", ValidOptions, provider));
        ex.Message.ShouldContain("services.AddHttpClient()");
    }

    [Test]
    public void Add_meilisearch_sink_registers_the_http_client_factory_itself()
    {
        var services = new ServiceCollection();
        var builder = new WallabyBuilder(services); // as constructed by the eager AddWallaby overload

        builder.AddMeilisearchSink("meili", o => o.Host = "http://localhost:7700");

        using var provider = services.BuildServiceProvider();
        provider.GetService<IHttpMessageHandlerFactory>().ShouldNotBeNull();
    }

    [Test]
    public void Sink_resolves_when_the_factory_is_registered()
    {
        var services = new ServiceCollection();
        services.AddHttpClient();
        using var provider = services.BuildServiceProvider();

        MeilisearchBuilderExtensions.CreateSink("meili", ValidOptions, provider).Name.ShouldBe("meili");
    }

    [Test]
    public async Task Named_client_configuration_is_honored()
    {
        var stub = new StubHandler();
        var services = new ServiceCollection();
        services.AddHttpClient(MeilisearchSink.ClientNameFor("meili"))
            .ConfigurePrimaryHttpMessageHandler(() => stub);
        using var provider = services.BuildServiceProvider();
        var sink = MeilisearchBuilderExtensions.CreateSink("meili", ValidOptions, provider);

        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        stub.Requests.ShouldNotBeEmpty(); // requests flowed through the named client's pipeline
    }

    [Test]
    [Arguments("")]
    [Arguments("meili/relative")]
    public void Host_must_be_an_absolute_url(string host)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddMeilisearchSink("meili", o => o.Host = host))
            .Message.ShouldContain("absolute");
    }

    [Test]
    [Arguments(0d)]
    [Arguments(-1d)]
    public void Wait_timeout_must_be_positive(double waitTimeoutMs)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Host = "http://localhost:7700";
            o.WaitTimeoutMs = waitTimeoutMs;
        })).Message.ShouldContain("WaitTimeoutMs");
    }

    [Test]
    public void Wait_interval_must_be_positive()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Host = "http://localhost:7700";
            o.WaitIntervalMs = 0;
        })).Message.ShouldContain("WaitIntervalMs");
    }

    [Test]
    public void Primary_key_is_required()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Host = "http://localhost:7700";
            o.PrimaryKey = " ";
        })).Message.ShouldContain("PrimaryKey");
    }

    [Test]
    public void Configured_index_name_is_required()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Host = "http://localhost:7700";
            o.ConfigureIndex("");
        })).Message.ShouldContain("Name");
    }

    private sealed record HostSetting(string Url);

    [Test]
    public async Task Provider_aware_overload_binds_option_values_at_first_resolution()
    {
        var stub = new StubHandler();
        var services = new ServiceCollection();
        services.AddHttpClient(MeilisearchSink.ClientNameFor("meili")).ConfigurePrimaryHttpMessageHandler(() => stub);
        services.AddSingleton(new HostSetting("http://meili.local"));
        var builder = new WallabyBuilder(services);

        var registration = builder
            .AddMeilisearchSink("meili", (sp, o) => o.Host = sp.GetRequiredService<HostSetting>().Url)
            .Registration;

        using var provider = services.BuildServiceProvider();
        var sink = registration.Factory(provider);
        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        stub.Requests.ShouldNotBeEmpty();
    }

    [Test]
    public void Provider_aware_overload_validates_at_first_resolution()
    {
        var services = new ServiceCollection();
        var builder = new WallabyBuilder(services); // registers AddHttpClient eagerly

        var registration = builder
            .AddMeilisearchSink("meili", (_, o) => o.Host = "meili/relative")
            .Registration;

        using var provider = services.BuildServiceProvider();
        Should.Throw<ArgumentException>(() => registration.Factory(provider))
            .Message.ShouldContain("absolute");
    }
}
