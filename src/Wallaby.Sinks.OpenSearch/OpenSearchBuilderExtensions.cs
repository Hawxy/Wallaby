using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.OpenSearch;

/// <summary>Fluent helpers for registering an OpenSearch sink on a <see cref="WallabyBuilder"/>.</summary>
public static class OpenSearchBuilderExtensions
{
    /// <summary>
    /// Register an OpenSearch sink under <paramref name="name"/>. Attach the entities it indexes via
    /// <see cref="WallabySinkBuilder.WithMappings"/> on the returned builder, using destinations as
    /// index names.
    /// </summary>
    public static WallabySinkBuilder AddOpenSearchSink(this WallabyBuilder builder, string name, Action<OpenSearchSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new OpenSearchSinkOptions { Endpoint = "" };
        configure(options);
        if (!Uri.TryCreate(options.Endpoint, UriKind.Absolute, out _))
        {
            throw new ArgumentException("OpenSearchSinkOptions.Endpoint must be an absolute URL.", nameof(configure));
        }
        if (options.MaxActionsPerRequest <= 0)
        {
            throw new ArgumentException("OpenSearchSinkOptions.MaxActionsPerRequest must be positive.", nameof(configure));
        }
        if (options.Password is not null && options.Username is null)
        {
            throw new ArgumentException("OpenSearchSinkOptions.Password requires Username.", nameof(configure));
        }

        // Factory registration so the container disposes the sink (and its connection settings).
        return builder.AddSink(name, _ => new OpenSearchSink(name, options));
    }
}
