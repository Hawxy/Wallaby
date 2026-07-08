using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Wallaby.TraceDemo;

/// <summary>
/// A local Aspire Dashboard container that receives OTLP traces/metrics and serves the viewer UI.
/// Reused across runs (fixed name + <c>WithReuse</c>) and deliberately never disposed — disposing
/// stops the container, which discards its in-memory traces. It outlives the demo process so traces
/// stay browsable after exit; remove with <c>docker rm -f wallaby-trace-dashboard</c>.
/// </summary>
public sealed class AspireDashboard
{
    private const int UiPort = 18888;
    private const int OtlpHostPort = 4317;
    private const int OtlpContainerPort = 18889;

    private readonly IContainer _container = new ContainerBuilder("mcr.microsoft.com/dotnet/aspire-dashboard:9.5")
        .WithName("wallaby-trace-dashboard")
        .WithReuse(true)
        .WithEnvironment("DOTNET_DASHBOARD_UNSECURED_ALLOW_ANONYMOUS", "true")
        .WithPortBinding(UiPort, UiPort)
        .WithPortBinding(OtlpHostPort, OtlpContainerPort)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(UiPort)))
        .Build();

    /// <summary>The dashboard UI.</summary>
    public string UiUrl => $"http://localhost:{UiPort}";

    /// <summary>OTLP gRPC ingestion endpoint for exporters.</summary>
    public string OtlpEndpoint => $"http://localhost:{OtlpHostPort}";

    public Task StartAsync(CancellationToken ct = default) => _container.StartAsync(ct);
}
