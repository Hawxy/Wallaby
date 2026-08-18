using Dekaf.Protocol.Records;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
        Validate(options);

        // Registered as a factory so the sink can pick up the host's ILoggerFactory; validation has
        // already run, so registration failures still surface eagerly.
        return builder.AddSink(name, sp => new KafkaSink(name, options, producer: null, sp.GetService<ILoggerFactory>()));
    }

    /// <summary>
    /// Provider-aware overload: <paramref name="configure"/> runs on first resolution, so option values
    /// can come from services (e.g. <c>IConfiguration</c>) while the registration itself stays eager.
    /// Validation failures surface at host start rather than at registration.
    /// </summary>
    public static WallabySinkBuilder AddKafkaSink(this WallabyBuilder builder, string name, Action<IServiceProvider, KafkaSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.AddSink(name, sp =>
        {
            var options = new KafkaSinkOptions { BootstrapServers = "" };
            configure(sp, options);
            Validate(options);
            return new KafkaSink(name, options, producer: null, sp.GetService<ILoggerFactory>());
        });
    }

    private static void Validate(KafkaSinkOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            throw new ArgumentException("KafkaSinkOptions.BootstrapServers is required.", nameof(options));
        }
        if (options.Compression == CompressionType.Brotli)
        {
            throw new ArgumentException(
                "KafkaSinkOptions.Compression does not support Brotli; reference Dekaf.Compression.Brotli and register it via ConfigureProducer instead.",
                nameof(options));
        }
        if (options.MessageTimeoutMs <= 0)
        {
            throw new ArgumentException("KafkaSinkOptions.MessageTimeoutMs must be positive.", nameof(options));
        }
        if (options.LingerMs < 0)
        {
            throw new ArgumentException("KafkaSinkOptions.LingerMs cannot be negative.", nameof(options));
        }
        if (options.MessageTimeoutMs <= options.LingerMs)
        {
            throw new ArgumentException(
                "KafkaSinkOptions.MessageTimeoutMs must exceed LingerMs; the delivery ceiling covers the linger window plus at least one broker request.",
                nameof(options));
        }
        if (options.AdminTimeoutMs <= 0)
        {
            throw new ArgumentException("KafkaSinkOptions.AdminTimeoutMs must be positive.", nameof(options));
        }
        foreach (var topic in options.Topics)
        {
            if (string.IsNullOrWhiteSpace(topic.Name))
            {
                throw new ArgumentException("KafkaTopicConfig.Name is required.", nameof(options));
            }
            if (topic.Partitions <= 0)
            {
                throw new ArgumentException(
                    $"KafkaTopicConfig.Partitions must be positive for topic '{topic.Name}'.", nameof(options));
            }
            if (topic.ReplicationFactor is not -1 and <= 0)
            {
                throw new ArgumentException(
                    $"KafkaTopicConfig.ReplicationFactor must be positive or -1 (broker default) for topic '{topic.Name}'.",
                    nameof(options));
            }
        }
    }
}
