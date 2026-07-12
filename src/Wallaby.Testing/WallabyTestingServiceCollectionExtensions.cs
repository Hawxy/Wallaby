using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.Testing;

/// <summary>
/// Post-registration overrides for test hosts: swap a production sink for a test double or adjust
/// <see cref="WallabyOptions"/> after the application's own <c>AddWallaby</c> call has run. Designed for
/// <c>WebApplicationFactory.ConfigureTestServices</c> (which executes after the app's
/// <c>ConfigureServices</c>) but works with any <see cref="IServiceCollection"/>.
/// </summary>
public static class WallabyTestingServiceCollectionExtensions
{
    /// <summary>
    /// Replace the sink registered under <paramref name="name"/> with <paramref name="replacement"/>.
    /// The sink's attached mappings (its <c>WithMappings(...)</c> declarations, destinations included) are
    /// preserved — batches keep routing by the registration name and arrive at the replacement with their
    /// original <see cref="SinkRecord.Destination"/> values, so a <see cref="CaptureSink"/> sees exactly
    /// what the production sink would have received.
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
        ArgumentNullException.ThrowIfNull(replacement);
        return services.MutateConfiguration(nameof(ReplaceWallabySink), configuration =>
        {
            // Swap the factory in place so the registration keeps its attached mappings.
            var registration = configuration.Sinks.FirstOrDefault(s => s.Name == name);
            if (registration is null)
            {
                var registered = configuration.Sinks.Count == 0
                    ? "(none)"
                    : string.Join(", ", configuration.Sinks.Select(s => $"'{s.Name}'"));
                throw new InvalidOperationException(
                    $"No sink named '{name}' is registered with Wallaby. Registered sinks: {registered}.");
            }
            registration.Factory = _ => replacement;
        });
    }

    /// <summary>
    /// Override <see cref="WallabyOptions"/> for a test host — e.g. to point a test run at its own replication
    /// slot and publication so it cannot collide with other environments. Equivalent to
    /// <c>services.PostConfigure(configure)</c>: it runs after the application's own option configuration
    /// (the <c>AddWallaby</c> builder and any <c>Configure&lt;WallabyOptions&gt;</c> calls), and repeated calls
    /// compose in call order.
    /// </summary>
    /// <param name="services">The service collection <c>AddWallaby</c> was called on.</param>
    /// <param name="configure">Applied when the options first materialize.</param>
    /// <exception cref="InvalidOperationException"><c>AddWallaby</c> has not been called on <paramref name="services"/>.</exception>
    public static IServiceCollection ConfigureWallabyOptions(this IServiceCollection services, Action<WallabyOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        EnsureWallabyRegistered(services, nameof(ConfigureWallabyOptions));
        services.PostConfigure(configure);
        return services;
    }

    /// <summary>
    /// Apply <paramref name="mutate"/> to the registered <see cref="WallabyConfiguration"/> instance.
    /// Repeated calls compose in call order.
    /// </summary>
    private static IServiceCollection MutateConfiguration(
        this IServiceCollection services, string caller, Action<WallabyConfiguration> mutate)
    {
        // Walk backwards: the LAST registration wins for singleton resolution. Keyed descriptors are skipped —
        // their ImplementationInstance getter throws.
        for (var i = services.Count - 1; i >= 0; i--)
        {
            var descriptor = services[i];
            if (descriptor.ServiceType != typeof(WallabyConfiguration) || descriptor.IsKeyedService)
            {
                continue;
            }

            if (descriptor.ImplementationInstance is WallabyConfiguration instance)
            {
                mutate(instance);
                return services;
            }

            break;
        }

        throw NotRegistered(caller);
    }

    private static void EnsureWallabyRegistered(IServiceCollection services, string caller)
    {
        if (!services.Any(d => d.ServiceType == typeof(WallabyConfiguration) && !d.IsKeyedService))
        {
            throw NotRegistered(caller);
        }
    }

    private static InvalidOperationException NotRegistered(string caller) =>
        new($"{caller} requires AddWallaby to have been called first — no WallabyConfiguration registration was found.");
}
