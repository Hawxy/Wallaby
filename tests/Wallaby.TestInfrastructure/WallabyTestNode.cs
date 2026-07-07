using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Wallaby.TestInfrastructure;

/// <summary>
/// A started node for production-style <c>AddWallaby</c> end-to-end tests: builds the service provider,
/// starts every <see cref="IHostedService"/>, and stops/disposes everything on dispose. Disposal is
/// idempotent so a cluster test can stop one node mid-test and still <c>await using</c> it.
/// The <c>AddWallaby</c> registration itself stays in the test — this owns only the lifecycle.
/// </summary>
public sealed class WallabyTestNode : IAsyncDisposable
{
    private bool _disposed;

    private WallabyTestNode(ServiceProvider services) => Services = services;

    public ServiceProvider Services { get; }

    public static async Task<WallabyTestNode> StartAsync(IServiceCollection services)
    {
        var provider = services.BuildServiceProvider();
        try
        {
            foreach (var hosted in provider.GetServices<IHostedService>())
            {
                await hosted.StartAsync(CancellationToken.None);
            }
        }
        catch
        {
            await provider.DisposeAsync();
            throw;
        }
        return new WallabyTestNode(provider);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;

        foreach (var hosted in Services.GetServices<IHostedService>())
        {
            await hosted.StopAsync(CancellationToken.None);
        }
        await Services.DisposeAsync();
    }
}
