using Testcontainers.Kafka;
using TUnit.Core.Interfaces;

namespace Wallaby.Sinks.Kafka.Tests.Integration.Infrastructure;

/// <summary>A shared single-broker Kafka container for sink integration tests.</summary>
public sealed class KafkaFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly KafkaContainer _container = new KafkaBuilder("confluentinc/cp-kafka:7.8.0").Build();

    public string BootstrapServers => _container.GetBootstrapAddress().Replace("PLAINTEXT://", "", StringComparison.OrdinalIgnoreCase);

    public async Task InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
