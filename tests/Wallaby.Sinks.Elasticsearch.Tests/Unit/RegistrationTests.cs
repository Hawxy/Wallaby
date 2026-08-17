using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Elasticsearch.Tests.Unit;

/// <summary>Validation of <c>AddElasticsearchSink</c>.</summary>
public class RegistrationTests
{
    [Test]
    [Arguments("")]
    [Arguments("elasticsearch/relative")]
    [Arguments("localhost:9200")]
    [Arguments("ftp://elasticsearch.local")]
    public void Endpoint_must_be_an_absolute_http_url(string endpoint)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddElasticsearchSink("search", o => o.Endpoint = endpoint))
            .Message.ShouldContain("absolute");
    }

    [Test]
    public void Max_actions_per_request_must_be_positive()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddElasticsearchSink("search", o =>
        {
            o.Endpoint = "http://elasticsearch.local:9200";
            o.MaxActionsPerRequest = 0;
        }));
    }

    [Test]
    public void Timeout_must_be_positive()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddElasticsearchSink("search", o =>
        {
            o.Endpoint = "http://elasticsearch.local:9200";
            o.TimeoutMs = 0;
        }));
    }

    [Test]
    public void Password_requires_a_username()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddElasticsearchSink("search", o =>
        {
            o.Endpoint = "http://elasticsearch.local:9200";
            o.Password = "secret";
        })).Message.ShouldContain("Username");
    }

    [Test]
    public void Username_requires_a_password()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddElasticsearchSink("search", o =>
        {
            o.Endpoint = "http://elasticsearch.local:9200";
            o.Username = "elastic";
        })).Message.ShouldContain("Password");
    }

    [Test]
    public void Api_key_and_basic_auth_are_mutually_exclusive()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddElasticsearchSink("search", o =>
        {
            o.Endpoint = "http://elasticsearch.local:9200";
            o.Username = "elastic";
            o.ApiKey = "key";
        })).Message.ShouldContain("mutually exclusive");
    }
}
