using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.OpenSearch.Tests.Unit;

/// <summary>Validation of <c>AddOpenSearchSink</c>.</summary>
public class RegistrationTests
{
    [Test]
    [Arguments("")]
    [Arguments("opensearch/relative")]
    [Arguments("localhost:9200")]
    [Arguments("ftp://opensearch.local")]
    public void Endpoint_must_be_an_absolute_http_url(string endpoint)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddOpenSearchSink("search", o => o.Endpoint = endpoint))
            .Message.ShouldContain("absolute");
    }

    [Test]
    public void Max_records_per_request_must_be_positive()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddOpenSearchSink("search", o =>
        {
            o.Endpoint = "http://opensearch.local:9200";
            o.MaxRecordsPerRequest = 0;
        }));
    }

    [Test]
    public void Password_requires_a_username()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddOpenSearchSink("search", o =>
        {
            o.Endpoint = "http://opensearch.local:9200";
            o.Password = "secret";
        })).Message.ShouldContain("Username");
    }

    [Test]
    public void Username_requires_a_password()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddOpenSearchSink("search", o =>
        {
            o.Endpoint = "http://opensearch.local:9200";
            o.Username = "admin";
        })).Message.ShouldContain("Password");
    }

    [Test]
    public void The_constructor_validates_options()
    {
        Should.Throw<WallabyConfigurationException>(
            () => new OpenSearchSink("search", new OpenSearchSinkOptions { Endpoint = "opensearch/relative" }));
    }

    [Test]
    public void Configure_connection_rejects_the_built_in_credentials()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddOpenSearchSink("search", o =>
        {
            o.Endpoint = "http://opensearch.local:9200";
            o.ConfigureConnection = uri => new global::OpenSearch.Client.ConnectionSettings(uri);
            o.Username = "admin";
            o.Password = "secret";
        })).Message.ShouldContain("ConfigureConnection");
    }
}
