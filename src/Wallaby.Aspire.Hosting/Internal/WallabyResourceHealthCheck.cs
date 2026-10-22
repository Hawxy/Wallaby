using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Wallaby.Aspire.Hosting.Internal;

/// <summary>Grades the monitor's latest poll; never queries the database itself.</summary>
internal sealed class WallabyResourceHealthCheck(WallabyResource resource) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        => Task.FromResult(WallabyObservationMapper.Health(resource.Monitor.Latest));
}
