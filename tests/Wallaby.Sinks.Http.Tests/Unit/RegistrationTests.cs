using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Http.Tests.Unit;

/// <summary>Validation and container requirements of <c>AddHttpSink</c>.</summary>
public class RegistrationTests
{
    private static readonly HttpSinkOptions ValidOptions = new() { Endpoint = "https://receiver.example/hooks" };

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
