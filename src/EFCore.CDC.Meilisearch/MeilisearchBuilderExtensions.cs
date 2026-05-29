using EFCore.CDC.DependencyInjection;

namespace EFCore.CDC.Meilisearch;

/// <summary>Fluent helpers for registering a Meilisearch sink on a <see cref="CdcBuilder"/>.</summary>
public static class MeilisearchBuilderExtensions
{
    /// <summary>Register a Meilisearch sink under <paramref name="name"/>.</summary>
    public static CdcBuilder AddMeilisearchSink(this CdcBuilder builder, string name, Action<MeilisearchSinkOptions> configure)
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
