using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;
using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Http;

/// <summary>Fluent helpers for registering an HTTP sink on a <see cref="WallabyBuilder"/>.</summary>
public static class HttpSinkBuilderExtensions
{
    /// <summary>
    /// Register an HTTP sink under <paramref name="name"/>. Attach the entities it receives via
    /// <see cref="WallabySinkBuilder.WithMappings"/> on the returned builder. Requires
    /// <c>services.AddHttpClient()</c> (registered automatically when <see cref="WallabyBuilder.Services"/>
    /// is available); authentication is configured on the factory's named client
    /// (<see cref="HttpSink.ClientNameFor"/>).
    /// </summary>
    public static WallabySinkBuilder AddHttpSink(this WallabyBuilder builder, string name, Action<HttpSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new HttpSinkOptions { Endpoint = "" };
        configure(options);
        Validate(options);

        builder.Services.AddHttpClient();
        DisableRedirectsOnDefaultClient(builder.Services, name);
        return builder.AddSink(name, sp => CreateSink(name, options, sp));
    }

    /// <summary>
    /// Provider-aware overload: <paramref name="configure"/> runs on first resolution, so option values
    /// can come from services (e.g. <c>IConfiguration</c>) while the registration itself stays eager.
    /// Validation failures surface at host start rather than at registration.
    /// </summary>
    public static WallabySinkBuilder AddHttpSink(this WallabyBuilder builder, string name, Action<IServiceProvider, HttpSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddHttpClient();
        DisableRedirectsOnDefaultClient(builder.Services, name);
        return builder.AddSink(name, sp =>
        {
            var options = new HttpSinkOptions { Endpoint = "" };
            configure(sp, options);
            Validate(options);
            return CreateSink(name, options, sp);
        });
    }

    /// <summary>
    /// The sink issues only POSTs, and following a redirect rewrites POST→GET and drops the body while a
    /// 2xx from the target acks the batch, so redirect following is disabled on the sink's <em>default</em>
    /// named client. Applied by mutating the existing primary handler (never replacing it, so
    /// <c>ConfigurePrimaryHttpMessageHandler</c> customizations like certs and proxies survive), and only
    /// to the default client's name: a user-supplied <see cref="HttpSinkOptions.HttpClientName"/> may be
    /// shared with other consumers and is left alone.
    /// </summary>
    private static void DisableRedirectsOnDefaultClient(IServiceCollection services, string sinkName)
        => services.PostConfigure<HttpClientFactoryOptions>(HttpSink.ClientNameFor(sinkName), o =>
            o.HttpMessageHandlerBuilderActions.Add(b =>
            {
                switch (b.PrimaryHandler)
                {
                    case HttpClientHandler h: h.AllowAutoRedirect = false; break;
                    case SocketsHttpHandler s: s.AllowAutoRedirect = false; break;
                    // A custom primary handler (e.g. a test stub) has no redirect behavior to disable.
                }
            }));

    private static void Validate(HttpSinkOptions options)
    {
        if (!Uri.TryCreate(options.Endpoint, UriKind.Absolute, out _))
        {
            throw new ArgumentException("HttpSinkOptions.Endpoint must be an absolute URL.", nameof(options));
        }
        if (options.MaxRecordsPerRequest <= 0)
        {
            throw new ArgumentException("HttpSinkOptions.MaxRecordsPerRequest must be positive.", nameof(options));
        }
        if (options.TimeoutMs <= 0)
        {
            throw new ArgumentException("HttpSinkOptions.TimeoutMs must be positive.", nameof(options));
        }
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
