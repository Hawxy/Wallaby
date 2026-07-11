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

        return builder.AddSink(new KafkaSink(name, options));
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
            return new KafkaSink(name, options);
        });
    }

    private static void Validate(KafkaSinkOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            throw new ArgumentException("KafkaSinkOptions.BootstrapServers is required.", nameof(options));
        }
        if (options.MessageTimeoutMs <= 0)
        {
            throw new ArgumentException("KafkaSinkOptions.MessageTimeoutMs must be positive.", nameof(options));
        }
        if (options.LingerMs < 0)
        {
            throw new ArgumentException("KafkaSinkOptions.LingerMs cannot be negative.", nameof(options));
        }
        if (options.AdminTimeoutMs <= 0)
        {
            throw new ArgumentException("KafkaSinkOptions.AdminTimeoutMs must be positive.", nameof(options));
        }
    }
}
