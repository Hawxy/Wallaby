using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.Testing;

/// <summary>
/// Post-registration overrides for test hosts: swap a production sink for a test double or adjust
/// <see cref="CdcOptions"/> after the application's own <c>AddWallaby</c> call has run. Designed for
/// <c>WebApplicationFactory.ConfigureTestServices</c> (which executes after the app's
/// <c>ConfigureServices</c>) but works with any <see cref="IServiceCollection"/>.
/// </summary>
public static class WallabyTestingServiceCollectionExtensions
{
    /// <summary>
    /// Replace the sink registered under <paramref name="name"/> with <paramref name="replacement"/>.
    /// Mapping destinations declared via <c>Map&lt;T&gt;().ToSink(name, destination)</c> are preserved —
    /// batches keep routing by the registration name and arrive at the replacement with their original
    /// <see cref="SinkRecord.Destination"/> values, so a <see cref="CaptureSink"/> sees exactly what the
    /// production sink would have received.
    /// </summary>
    /// <param name="services">The service collection <c>AddWallaby</c> was called on.</param>
    /// <param name="name">The registration name of the sink to replace (e.g. <c>"meili"</c>).</param>
    /// <param name="replacement">The sink that should receive the batches instead.</param>
    /// <exception cref="InvalidOperationException">
    /// <c>AddWallaby</c> has not been called on <paramref name="services"/>, or no sink is registered
    /// under <paramref name="name"/>.
    /// </exception>
    public static IServiceCollection ReplaceWallabySink(this IServiceCollection services, string name, ISink replacement)
    {
        var configuration = FindInstance<CdcConfiguration>(services, nameof(ReplaceWallabySink));
        var removed = configuration.Sinks.RemoveAll(s => s.Name == name);
        if (removed == 0)
        {
            var registered = configuration.Sinks.Count == 0
                ? "(none)"
                : string.Join(", ", configuration.Sinks.Select(s => $"'{s.Name}'"));
            throw new InvalidOperationException(
                $"No sink named '{name}' is registered with Wallaby. Registered sinks: {registered}.");
        }
        configuration.Sinks.Add(new SinkRegistration { Name = name, Factory = _ => replacement });
        return services;
    }

    /// <summary>
    /// Mutate the <see cref="CdcOptions"/> instance registered by <c>AddWallaby</c> — e.g. to point a test
    /// run at its own replication slot and publication so it cannot collide with other environments.
    /// </summary>
    /// <param name="services">The service collection <c>AddWallaby</c> was called on.</param>
    /// <param name="configure">Applied immediately to the registered options instance.</param>
    /// <exception cref="InvalidOperationException"><c>AddWallaby</c> has not been called on <paramref name="services"/>.</exception>
    public static IServiceCollection ConfigureWallabyOptions(this IServiceCollection services, Action<CdcOptions> configure)
    {
        configure(FindInstance<CdcOptions>(services, nameof(ConfigureWallabyOptions)));
        return services;
    }

    private static T FindInstance<T>(IServiceCollection services, string caller) where T : class
    {
        var descriptor = services.LastOrDefault(d => d.ServiceType == typeof(T) && d.ImplementationInstance is T);
        return descriptor?.ImplementationInstance as T
            ?? throw new InvalidOperationException(
                $"{caller} requires AddWallaby to have been called first — no {typeof(T).Name} singleton instance is registered.");
    }
}
