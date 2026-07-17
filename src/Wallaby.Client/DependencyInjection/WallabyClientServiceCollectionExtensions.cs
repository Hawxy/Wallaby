using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Wallaby.Client.DependencyInjection;

/// <summary>Registers <see cref="WallabyControlClient"/> in a service collection.</summary>
public static class WallabyClientServiceCollectionExtensions
{
    /// <summary>
    /// Register a singleton <see cref="WallabyControlClient"/> over a data source the client owns, built
    /// from <paramref name="connectionString"/> and disposed with the container. Idempotent: a client
    /// already registered wins.
    /// </summary>
    public static IServiceCollection AddWallabyControlClient(
        this IServiceCollection services, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        services.TryAddSingleton(sp =>
            new WallabyControlClient(connectionString, sp.GetService<ILogger<WallabyControlClient>>()));
        return services;
    }

    /// <summary>
    /// Register a singleton <see cref="WallabyControlClient"/> over a data source supplied by
    /// <paramref name="dataSourceFactory"/>. The data source's lifetime stays with its owner; the client
    /// does not dispose it. Idempotent: a client already registered wins.
    /// </summary>
    public static IServiceCollection AddWallabyControlClient(
        this IServiceCollection services, Func<IServiceProvider, NpgsqlDataSource> dataSourceFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dataSourceFactory);
        services.TryAddSingleton(sp =>
            new WallabyControlClient(dataSourceFactory(sp), sp.GetService<ILogger<WallabyControlClient>>()));
        return services;
    }

    /// <summary>
    /// Register a singleton <see cref="WallabyControlClient"/> over the container's
    /// <see cref="NpgsqlDataSource"/> registration (e.g. from <c>AddNpgsqlDataSource</c>). Idempotent:
    /// a client already registered wins.
    /// </summary>
    public static IServiceCollection AddWallabyControlClient(this IServiceCollection services)
        => services.AddWallabyControlClient(sp => sp.GetRequiredService<NpgsqlDataSource>());
}
