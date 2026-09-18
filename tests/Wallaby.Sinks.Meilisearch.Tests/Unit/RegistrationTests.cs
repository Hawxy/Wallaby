using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using static Wallaby.Sinks.Meilisearch.Tests.Unit.MeilisearchTestHelpers;

namespace Wallaby.Sinks.Meilisearch.Tests.Unit;

/// <summary>Validation and container requirements of <c>AddMeilisearchSink</c>.</summary>
public class RegistrationTests
{
    private static readonly MeilisearchSinkOptions ValidOptions = new() { Endpoint = "http://localhost:7700" };

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

        builder.AddMeilisearchSink("meili", o => o.Endpoint = "http://localhost:7700");

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
    [Arguments("ftp://meili.local")]
    public void Endpoint_must_be_an_absolute_http_url(string endpoint)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddMeilisearchSink("meili", o => o.Endpoint = endpoint))
            .Message.ShouldContain("absolute http(s)");
    }

    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public void Wait_timeout_must_be_positive(int waitTimeoutSeconds)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Endpoint = "http://localhost:7700";
            o.WaitTimeout = TimeSpan.FromSeconds(waitTimeoutSeconds);
        })).Message.ShouldContain("WaitTimeout");
    }

    [Test]
    public void Wait_interval_must_be_positive()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Endpoint = "http://localhost:7700";
            o.WaitInterval = TimeSpan.Zero;
        })).Message.ShouldContain("WaitInterval");
    }

    [Test]
    public void The_constructor_validates_options()
    {
        var services = new ServiceCollection();
        services.AddHttpClient();
        using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IHttpMessageHandlerFactory>();

        Should.Throw<WallabyConfigurationException>(
            () => new MeilisearchSink("meili", new MeilisearchSinkOptions { Endpoint = "meili/relative" }, factory));
    }

    [Test]
    public void Primary_key_is_required()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Endpoint = "http://localhost:7700";
            o.PrimaryKey = " ";
        })).Message.ShouldContain("PrimaryKey");
    }

    [Test]
    public void Configured_index_name_is_required()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Endpoint = "http://localhost:7700";
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
            .AddMeilisearchSink("meili", (sp, o) => o.Endpoint = sp.GetRequiredService<HostSetting>().Url)
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
            .AddMeilisearchSink("meili", (_, o) => o.Endpoint = "meili/relative")
            .Registration;

        using var provider = services.BuildServiceProvider();
        Should.Throw<WallabyConfigurationException>(() => registration.Factory(provider))
            .Message.ShouldContain("absolute");
    }
}
