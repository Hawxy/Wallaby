using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using TUnit.Core.Interfaces;

namespace Wallaby.Sinks.Meilisearch.Tests.Integration.Infrastructure;

/// <summary>A shared Meilisearch container for sink integration tests.</summary>
public sealed class MeilisearchFixture : IAsyncInitializer, IAsyncDisposable
{
    private const string MasterKey = "masterKey";

    private readonly IContainer _container = new ContainerBuilder("getmeili/meilisearch:v1.45.1")
        .WithEnvironment("MEILI_MASTER_KEY", MasterKey)
        .WithEnvironment("MEILI_ENV", "development")
        .WithPortBinding(7700, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(7700).ForPath("/health")))
        .Build();

    public string ApiKey => MasterKey;

    public string Host => $"http://{_container.Hostname}:{_container.GetMappedPublicPort(7700)}";

    public async Task InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
