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
        Validate(options);

        // Factory registration so the container disposes the sink (and its connection settings).
        return builder.AddSink(name, _ => new OpenSearchSink(name, options));
    }

    /// <summary>
    /// Provider-aware overload: <paramref name="configure"/> runs on first resolution, so option values
    /// can come from services (e.g. <c>IConfiguration</c>) while the registration itself stays eager.
    /// Validation failures surface at host start rather than at registration.
    /// </summary>
    public static WallabySinkBuilder AddOpenSearchSink(this WallabyBuilder builder, string name, Action<IServiceProvider, OpenSearchSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.AddSink(name, sp =>
        {
            var options = new OpenSearchSinkOptions { Endpoint = "" };
            configure(sp, options);
            return new OpenSearchSink(name, options);
        });
    }

    internal static void Validate(OpenSearchSinkOptions options)
    {
        if (!Uri.TryCreate(options.Endpoint, UriKind.Absolute, out var endpoint)
            || (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps))
        {
            throw new WallabyConfigurationException("OpenSearchSinkOptions.Endpoint must be an absolute http(s) URL.");
        }
        if (options.MaxRecordsPerRequest <= 0)
        {
            throw new WallabyConfigurationException("OpenSearchSinkOptions.MaxRecordsPerRequest must be positive.");
        }
        if (options.Timeout <= TimeSpan.Zero)
        {
            throw new WallabyConfigurationException("OpenSearchSinkOptions.Timeout must be positive.");
        }
        if (options.ConfigureConnection is not null && (options.Username is not null || options.Password is not null))
        {
            throw new WallabyConfigurationException(
                "OpenSearchSinkOptions.ConfigureConnection replaces the built-in authentication: leave Username and " +
                "Password unset and configure authentication on the returned settings.");
        }
        if (options.Password is not null && options.Username is null)
        {
            throw new WallabyConfigurationException("OpenSearchSinkOptions.Password requires Username.");
        }
        if (options.Username is not null && options.Password is null)
        {
            throw new WallabyConfigurationException("OpenSearchSinkOptions.Username requires Password.");
        }
    }
}
