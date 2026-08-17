using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Elasticsearch;

/// <summary>Fluent helpers for registering an Elasticsearch sink on a <see cref="WallabyBuilder"/>.</summary>
public static class ElasticsearchBuilderExtensions
{
    /// <summary>
    /// Register an Elasticsearch sink under <paramref name="name"/>. Attach the entities it indexes via
    /// <see cref="WallabySinkBuilder.WithMappings"/> on the returned builder, using destinations as
    /// index names.
    /// </summary>
    public static WallabySinkBuilder AddElasticsearchSink(this WallabyBuilder builder, string name, Action<ElasticsearchSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new ElasticsearchSinkOptions { Endpoint = "" };
        configure(options);
        Validate(options);

        // Factory registration so the container disposes the sink (and its connection settings).
        return builder.AddSink(name, _ => new ElasticsearchSink(name, options));
    }

    /// <summary>
    /// Provider-aware overload: <paramref name="configure"/> runs on first resolution, so option values
    /// can come from services (e.g. <c>IConfiguration</c>) while the registration itself stays eager.
    /// Validation failures surface at host start rather than at registration.
    /// </summary>
    public static WallabySinkBuilder AddElasticsearchSink(this WallabyBuilder builder, string name, Action<IServiceProvider, ElasticsearchSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.AddSink(name, sp =>
        {
            var options = new ElasticsearchSinkOptions { Endpoint = "" };
            configure(sp, options);
            Validate(options);
            return new ElasticsearchSink(name, options);
        });
    }

    private static void Validate(ElasticsearchSinkOptions options)
    {
        if (!Uri.TryCreate(options.Endpoint, UriKind.Absolute, out var endpoint)
            || (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps))
        {
            throw new ArgumentException("ElasticsearchSinkOptions.Endpoint must be an absolute http(s) URL.", nameof(options));
        }
        if (options.MaxActionsPerRequest <= 0)
        {
            throw new ArgumentException("ElasticsearchSinkOptions.MaxActionsPerRequest must be positive.", nameof(options));
        }
        if (options.TimeoutMs <= 0)
        {
            throw new ArgumentException("ElasticsearchSinkOptions.TimeoutMs must be positive.", nameof(options));
        }
        if (options.ApiKey is not null && options.Username is not null)
        {
            throw new ArgumentException(
                "ElasticsearchSinkOptions.ApiKey and Username are mutually exclusive; configure one authentication scheme.",
                nameof(options));
        }
        if (options.Password is not null && options.Username is null)
        {
            throw new ArgumentException("ElasticsearchSinkOptions.Password requires Username.", nameof(options));
        }
        if (options.Username is not null && options.Password is null)
        {
            throw new ArgumentException("ElasticsearchSinkOptions.Username requires Password.", nameof(options));
        }
    }
}
