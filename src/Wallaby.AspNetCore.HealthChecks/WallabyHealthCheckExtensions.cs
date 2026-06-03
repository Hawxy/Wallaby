using Microsoft.Extensions.Diagnostics.HealthChecks;
using Wallaby.AspNetCore.HealthChecks;

// ReSharper disable once CheckNamespace — extension lives in the DI namespace so it's discoverable without an extra using.
namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers Wallaby's health check.</summary>
public static class WallabyHealthCheckExtensions
{
    /// <summary>
    /// Add Wallaby's liveness health check, which is Unhealthy only when the CDC subsystem has terminated
    /// (a healthy standby or a lagging leader stays Healthy). Wire it to a liveness probe.
    /// </summary>
    /// <param name="builder">The health-checks builder.</param>
    /// <param name="name">Registration name (default <c>wallaby</c>).</param>
    /// <param name="tags">Tags for filtering probes (default <c>wallaby</c>).</param>
    public static IHealthChecksBuilder AddWallaby(
        this IHealthChecksBuilder builder,
        string name = "wallaby",
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.AddCheck<WallabyHealthCheck>(name, failureStatus: HealthStatus.Unhealthy, tags: tags ?? ["wallaby"]);
        return builder;
    }
}
