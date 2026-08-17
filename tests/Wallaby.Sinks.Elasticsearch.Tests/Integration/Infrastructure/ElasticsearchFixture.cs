using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using TUnit.Core.Interfaces;

namespace Wallaby.Sinks.Elasticsearch.Tests.Integration.Infrastructure;

/// <summary>A shared single-node Elasticsearch container (security disabled) for sink integration tests.</summary>
public sealed class ElasticsearchFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly IContainer _container = new ContainerBuilder("docker.elastic.co/elasticsearch/elasticsearch:9.5.0")
        .WithEnvironment("discovery.type", "single-node")
        .WithEnvironment("xpack.security.enabled", "false")
        .WithPortBinding(9200, true)
        // "/" responds before the cluster state is recovered (document APIs still 503); wait for real health.
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(
            r => r.ForPort(9200).ForPath("/_cluster/health").ForResponseMessageMatching(IsReadyAsync),
            o => o.WithTimeout(TimeSpan.FromMinutes(5))))
        .Build();

    private static async Task<bool> IsReadyAsync(HttpResponseMessage response)
    {
        var body = await response.Content.ReadAsStringAsync();
        return body.Contains("\"status\":\"green\"") || body.Contains("\"status\":\"yellow\"");
    }

    public string Endpoint => $"http://{_container.Hostname}:{_container.GetMappedPublicPort(9200)}";

    public async Task InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
