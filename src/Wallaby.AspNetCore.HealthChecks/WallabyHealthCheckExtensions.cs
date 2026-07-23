using Microsoft.Extensions.Diagnostics.HealthChecks;
using Wallaby.Abstractions;
using Wallaby.AspNetCore.HealthChecks;

// ReSharper disable once CheckNamespace
// The extension lives in the DI namespace so it's discoverable without an extra using.
namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers Wallaby's health check.</summary>
public static class WallabyHealthCheckExtensions
{
    /// <summary>
    /// Add Wallaby's liveness health check, which is Unhealthy when the Wallaby subsystem has terminated or
    /// the leader is crash-looping (<see cref="WallabyHealthCheckOptions.CrashLoopFailureThreshold"/>
    /// consecutive failures without progress); a healthy standby or a lagging leader stays Healthy.
    /// Wire it to a liveness probe.
    /// </summary>
    /// <param name="builder">The health-checks builder.</param>
    /// <param name="name">Registration name (default <c>wallaby</c>).</param>
    /// <param name="tags">Tags for filtering probes (default <c>wallaby</c>).</param>
    /// <param name="configure">Optional adjustment of the check's grading thresholds.</param>
    public static IHealthChecksBuilder AddWallaby(
        this IHealthChecksBuilder builder,
        string name = "wallaby",
        IEnumerable<string>? tags = null,
        Action<WallabyHealthCheckOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new WallabyHealthCheckOptions();
        configure?.Invoke(options);

        builder.Add(new HealthCheckRegistration(
            name,
            sp => new WallabyHealthCheck(sp.GetRequiredService<IWallabyStatus>(), options),
            failureStatus: HealthStatus.Unhealthy,
            tags: tags ?? ["wallaby"]));
        return builder;
    }
}
