using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Kafka;

/// <summary>Fluent helpers for registering a Kafka sink on a <see cref="WallabyBuilder"/>.</summary>
public static class KafkaBuilderExtensions
{
    /// <summary>
    /// Register a Kafka sink under <paramref name="name"/>. Attach the entities it produces via
    /// <see cref="WallabySinkBuilder.WithMappings"/> on the returned builder.
    /// </summary>
    public static WallabySinkBuilder AddKafkaSink(this WallabyBuilder builder, string name, Action<KafkaSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new KafkaSinkOptions { BootstrapServers = "" };
        configure(options);
        if (string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            throw new ArgumentException("KafkaSinkOptions.BootstrapServers is required.", nameof(configure));
        }
        if (options.MessageTimeoutMs <= 0)
        {
            throw new ArgumentException("KafkaSinkOptions.MessageTimeoutMs must be positive.", nameof(configure));
        }
        if (options.LingerMs < 0)
        {
            throw new ArgumentException("KafkaSinkOptions.LingerMs cannot be negative.", nameof(configure));
        }

        return builder.AddSink(new KafkaSink(name, options));
    }
}
