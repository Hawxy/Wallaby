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

        var options = new MeilisearchSinkOptions { Host = "" };
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
            var options = new MeilisearchSinkOptions { Host = "" };
            configure(sp, options);
            Validate(options);
            return CreateSink(name, options, sp);
        });
    }

    private static void Validate(MeilisearchSinkOptions options)
    {
        if (!Uri.TryCreate(options.Host, UriKind.Absolute, out _))
        {
            throw new ArgumentException("MeilisearchSinkOptions.Host must be an absolute URL.", nameof(options));
        }
        if (options.MaxRecordsPerBatch <= 0)
        {
            throw new ArgumentException("MeilisearchSinkOptions.MaxRecordsPerBatch must be positive.", nameof(options));
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
