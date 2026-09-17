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
            return new KafkaSink(name, options, producer: null, sp.GetService<ILoggerFactory>());
        });
    }

    internal static void Validate(KafkaSinkOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            throw new WallabyConfigurationException("KafkaSinkOptions.BootstrapServers is required.");
        }
        if (!Enum.IsDefined(options.Compression))
        {
            throw new WallabyConfigurationException(
                $"KafkaSinkOptions.Compression value {(int)options.Compression} is not a defined KafkaSinkCompression.");
        }
        if (options.MessageTimeout <= TimeSpan.Zero)
        {
            throw new WallabyConfigurationException("KafkaSinkOptions.MessageTimeout must be positive.");
        }
        if (options.Linger < TimeSpan.Zero)
        {
            throw new WallabyConfigurationException("KafkaSinkOptions.Linger cannot be negative.");
        }
        if (options.MessageTimeout <= options.Linger)
        {
            throw new WallabyConfigurationException(
                "KafkaSinkOptions.MessageTimeout must exceed Linger; the delivery ceiling covers the linger window plus at least one broker request.");
        }
        if (options.AdminTimeout <= TimeSpan.Zero)
        {
            throw new WallabyConfigurationException("KafkaSinkOptions.AdminTimeout must be positive.");
        }
        foreach (var topic in options.Topics)
        {
            if (string.IsNullOrWhiteSpace(topic.Name))
            {
                throw new WallabyConfigurationException("KafkaTopicConfig.Name is required.");
            }
            if (topic.Partitions <= 0)
            {
                throw new WallabyConfigurationException(
                    $"KafkaTopicConfig.Partitions must be positive for topic '{topic.Name}'.");
            }
            if (topic.ReplicationFactor is not -1 and <= 0)
            {
                throw new WallabyConfigurationException(
                    $"KafkaTopicConfig.ReplicationFactor must be positive or -1 (broker default) for topic '{topic.Name}'.");
            }
        }
    }
}
