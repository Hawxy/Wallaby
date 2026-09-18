using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Meilisearch;

/// <summary>Fluent helpers for registering a Meilisearch sink on a <see cref="WallabyBuilder"/>.</summary>
public static class MeilisearchBuilderExtensions
{
    /// <summary>
    /// Register a Meilisearch sink under <paramref name="name"/>. Attach the entities it indexes via
    /// <see cref="WallabySinkBuilder.WithMappings"/> on the returned builder. Requires
    /// <c>services.AddHttpClient()</c> (registered automatically when <see cref="WallabyBuilder.Services"/>
    /// is available); the HTTP pipeline is configured on the factory's named client
    /// (<see cref="MeilisearchSink.ClientNameFor"/>).
    /// </summary>
    public static WallabySinkBuilder AddMeilisearchSink(this WallabyBuilder builder, string name, Action<MeilisearchSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new MeilisearchSinkOptions { Endpoint = "" };
        configure(options);
        Validate(options);

        builder.Services.AddHttpClient();
        return builder.AddSink(name, sp => CreateSink(name, options, sp));
    }

    /// <summary>
    /// Provider-aware overload: <paramref name="configure"/> runs on first resolution, so option values
    /// can come from services (e.g. <c>IConfiguration</c>) while the registration itself stays eager.
    /// Validation failures surface at host start rather than at registration.
    /// </summary>
    public static WallabySinkBuilder AddMeilisearchSink(this WallabyBuilder builder, string name, Action<IServiceProvider, MeilisearchSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddHttpClient();
        return builder.AddSink(name, sp =>
        {
            var options = new MeilisearchSinkOptions { Endpoint = "" };
            configure(sp, options);
            return CreateSink(name, options, sp);
        });
    }

    internal static void Validate(MeilisearchSinkOptions options)
    {
        if (!Uri.TryCreate(options.Endpoint, UriKind.Absolute, out var endpoint)
            || (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps))
        {
            throw new WallabyConfigurationException("MeilisearchSinkOptions.Endpoint must be an absolute http(s) URL.");
        }
        if (options.MaxRecordsPerRequest <= 0)
        {
            throw new WallabyConfigurationException("MeilisearchSinkOptions.MaxRecordsPerRequest must be positive.");
        }
        if (options.WaitTimeout <= TimeSpan.Zero)
        {
            throw new WallabyConfigurationException("MeilisearchSinkOptions.WaitTimeout must be positive.");
        }
        if (options.WaitInterval <= TimeSpan.Zero)
        {
            throw new WallabyConfigurationException("MeilisearchSinkOptions.WaitInterval must be positive.");
        }
        if (string.IsNullOrWhiteSpace(options.PrimaryKey))
        {
            throw new WallabyConfigurationException("MeilisearchSinkOptions.PrimaryKey is required.");
        }
        foreach (var index in options.Indexes)
        {
            if (string.IsNullOrWhiteSpace(index.Name))
            {
                throw new WallabyConfigurationException("MeilisearchIndexConfig.Name is required.");
            }
        }
    }

    internal static MeilisearchSink CreateSink(string name, MeilisearchSinkOptions options, IServiceProvider services)
    {
        var factory = services.GetService<IHttpMessageHandlerFactory>()
            ?? throw new WallabyConfigurationException(
                $"AddMeilisearchSink(\"{name}\") requires IHttpMessageHandlerFactory. Call services.AddHttpClient() " +
                "(Microsoft.Extensions.Http) when registering services.");
        return new MeilisearchSink(name, options, factory);
    }
}
