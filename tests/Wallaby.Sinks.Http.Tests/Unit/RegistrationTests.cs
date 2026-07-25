using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Options;
using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Http.Tests.Unit;

/// <summary>Validation and container requirements of <c>AddHttpSink</c>.</summary>
public class RegistrationTests
{
    private static readonly HttpSinkOptions ValidOptions = new() { Endpoint = "https://receiver.example/hooks" };

    private static HttpMessageHandler ApplyHandlerActions(string clientName, HttpMessageHandler primary)
    {
        var services = new ServiceCollection();
        new WallabyBuilder(services).AddHttpSink("webhook", o => o.Endpoint = "https://receiver.example/hooks");
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptionsMonitor<HttpClientFactoryOptions>>().Get(clientName);
        var builder = new TestHandlerBuilder { PrimaryHandler = primary };
        foreach (var action in options.HttpMessageHandlerBuilderActions)
        {
            action(builder);
        }
        return builder.PrimaryHandler;
    }

    [Test]
    public void Default_client_disables_redirect_following()
    {
        var primary = new HttpClientHandler(); // factory default: AllowAutoRedirect = true
        var result = ApplyHandlerActions(HttpSink.ClientNameFor("webhook"), primary);

        result.ShouldBeSameAs(primary); // mutated, never replaced (user cert/proxy config survives)
        primary.AllowAutoRedirect.ShouldBeFalse();
    }

    [Test]
    public void A_custom_primary_handler_is_left_untouched()
    {
        var stub = new StubHandler();

        ApplyHandlerActions(HttpSink.ClientNameFor("webhook"), stub).ShouldBeSameAs(stub);
    }

    [Test]
    public void Other_client_names_are_not_reconfigured()
    {
        // A user-supplied HttpClientName may be shared with other consumers; only the sink's own
        // conventional client name gets the redirect opt-out.
        var primary = new HttpClientHandler();
        ApplyHandlerActions("custom", primary);

        primary.AllowAutoRedirect.ShouldBeTrue();
    }

    private sealed class StubHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => throw new NotSupportedException();
    }

    private sealed class TestHandlerBuilder : HttpMessageHandlerBuilder
    {
        public override string? Name { get; set; }
        public override HttpMessageHandler PrimaryHandler { get; set; } = new HttpClientHandler();
        public override IList<DelegatingHandler> AdditionalHandlers { get; } = [];
        public override HttpMessageHandler Build() => PrimaryHandler;
    }

    [Test]
    public void Sink_resolution_requires_the_http_client_factory()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();

        var ex = Should.Throw<WallabyConfigurationException>(
            () => HttpSinkBuilderExtensions.CreateSink("webhook", ValidOptions, provider));
        ex.Message.ShouldContain("services.AddHttpClient()");
    }

    [Test]
    public void Sink_resolves_when_the_factory_is_registered()
    {
        var services = new ServiceCollection();
        services.AddHttpClient();
        using var provider = services.BuildServiceProvider();

        HttpSinkBuilderExtensions.CreateSink("webhook", ValidOptions, provider).Name.ShouldBe("webhook");
    }

    [Test]
    [Arguments("")]
    [Arguments("hooks/relative")]
    public void Endpoint_must_be_an_absolute_url(string endpoint)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddHttpSink("webhook", o => o.Endpoint = endpoint))
            .Message.ShouldContain("absolute");
    }

    [Test]
    public void Max_records_per_request_must_be_positive()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddHttpSink("webhook", o =>
        {
            o.Endpoint = "https://receiver.example/hooks";
            o.MaxRecordsPerRequest = 0;
        }));
    }

    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public void Timeout_must_be_positive(int timeoutMs)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddHttpSink("webhook", o =>
        {
            o.Endpoint = "https://receiver.example/hooks";
            o.TimeoutMs = timeoutMs;
        })).Message.ShouldContain("TimeoutMs");
    }
}
