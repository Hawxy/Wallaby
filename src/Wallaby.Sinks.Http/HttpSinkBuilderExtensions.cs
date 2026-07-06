using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Http;

/// <summary>Fluent helpers for registering an HTTP sink on a <see cref="WallabyBuilder"/>.</summary>
public static class HttpSinkBuilderExtensions
{
    /// <summary>
    /// Register an HTTP sink under <paramref name="name"/>. Attach the entities it receives via
    /// <see cref="WallabySinkBuilder.WithMappings"/> on the returned builder. Requires
    /// <c>services.AddHttpClient()</c>; authentication is configured on the factory's named client
    /// (<see cref="HttpSink.ClientNameFor"/>).
    /// </summary>
    public static WallabySinkBuilder AddHttpSink(this WallabyBuilder builder, string name, Action<HttpSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new HttpSinkOptions { Endpoint = "" };
        configure(options);
        if (!Uri.TryCreate(options.Endpoint, UriKind.Absolute, out _))
        {
            throw new ArgumentException("HttpSinkOptions.Endpoint must be an absolute URL.", nameof(configure));
        }
        if (options.MaxRecordsPerRequest <= 0)
        {
            throw new ArgumentException("HttpSinkOptions.MaxRecordsPerRequest must be positive.", nameof(configure));
        }

        return builder.AddSink(name, sp => CreateSink(name, options, sp));
    }

    internal static HttpSink CreateSink(string name, HttpSinkOptions options, IServiceProvider services)
    {
        var factory = services.GetService<IHttpClientFactory>()
            ?? throw new WallabyConfigurationException(
                $"AddHttpSink(\"{name}\") requires IHttpClientFactory. Call services.AddHttpClient() " +
                "(Microsoft.Extensions.Http) when registering services.");
        return new HttpSink(name, options, factory);
    }
}
