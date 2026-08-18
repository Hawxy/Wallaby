using Testcontainers.Kafka;
using TUnit.Core.Interfaces;

namespace Wallaby.Sinks.Kafka.Tests.Integration.Infrastructure;

/// <summary>A shared single-broker Kafka container for sink integration tests.</summary>
public sealed class KafkaFixture : IAsyncInitializer, IAsyncDisposable
{
    // The extra listener works around Testcontainers 4.14.0 emitting a trailing comma in
    // KAFKA_ADVERTISED_LISTENERS when no listener is declared; Kafka 4.x rejects the resulting
    // empty entry ("values must not be empty") and the broker never starts. It must be a
    // resolvable bind address (localhost), not a network alias: without a custom Docker network
    // the broker cannot bind an alias hostname and dies with "Unable to start acceptor".
    private readonly KafkaContainer _container = new KafkaBuilder("apache/kafka:4.3.1")
        .WithListener("localhost:19092")
        .Build();

    public string BootstrapServers => _container.GetBootstrapAddress().Replace("PLAINTEXT://", "", StringComparison.OrdinalIgnoreCase);

    public async Task InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
