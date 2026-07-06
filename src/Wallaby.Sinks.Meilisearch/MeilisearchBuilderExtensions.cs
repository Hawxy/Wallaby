using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Meilisearch;

/// <summary>Fluent helpers for registering a Meilisearch sink on a <see cref="WallabyBuilder"/>.</summary>
public static class MeilisearchBuilderExtensions
{
    /// <summary>
    /// Register a Meilisearch sink under <paramref name="name"/>. Attach the entities it indexes via
    /// <see cref="WallabySinkBuilder.WithMappings"/> on the returned builder.
    /// </summary>
    public static WallabySinkBuilder AddMeilisearchSink(this WallabyBuilder builder, string name, Action<MeilisearchSinkOptions> configure)
    {
        var options = new MeilisearchSinkOptions { Host = "" };
        configure(options);
        if (string.IsNullOrWhiteSpace(options.Host))
        {
            throw new ArgumentException("MeilisearchSinkOptions.Host is required.", nameof(configure));
        }

        return builder.AddSink(new MeilisearchSink(name, options));
    }
}
