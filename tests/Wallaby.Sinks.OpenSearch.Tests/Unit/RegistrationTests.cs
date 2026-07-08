using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.OpenSearch.Tests.Unit;

/// <summary>Validation of <c>AddOpenSearchSink</c>.</summary>
public class RegistrationTests
{
    [Test]
    [Arguments("")]
    [Arguments("opensearch/relative")]
    public void Endpoint_must_be_an_absolute_url(string endpoint)
    {
        var builder = new WallabyBuilder();

        Should.Throw<ArgumentException>(() => builder.AddOpenSearchSink("search", o => o.Endpoint = endpoint))
            .Message.ShouldContain("absolute");
    }

    [Test]
    public void Max_actions_per_request_must_be_positive()
    {
        var builder = new WallabyBuilder();

        Should.Throw<ArgumentException>(() => builder.AddOpenSearchSink("search", o =>
        {
            o.Endpoint = "http://opensearch.local:9200";
            o.MaxActionsPerRequest = 0;
        }));
    }

    [Test]
    public void Password_requires_a_username()
    {
        var builder = new WallabyBuilder();

        Should.Throw<ArgumentException>(() => builder.AddOpenSearchSink("search", o =>
        {
            o.Endpoint = "http://opensearch.local:9200";
            o.Password = "secret";
        })).Message.ShouldContain("Username");
    }
}
