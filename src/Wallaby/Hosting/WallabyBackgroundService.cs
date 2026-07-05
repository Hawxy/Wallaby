using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wallaby.Diagnostics;

namespace Wallaby.Hosting;

/// <summary>Hosts the <see cref="WallabyRuntime"/> as a long-running background service.</summary>
internal sealed class WallabyBackgroundService(
    WallabyRuntime runtime, WallabyStatus status, ILogger<WallabyBackgroundService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await runtime.RunAsync(stoppingToken);
            status.MarkStopped();
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            status.MarkStopped(); // graceful shutdown
        }
        catch (Exception ex)
        {
            // The only reliable "Wallaby died" signal — BackgroundService exposes no queryable terminated flag.
            status.MarkFaulted($"{ex.GetType().Name}: {ex.Message}");
            logger.BackgroundServiceTerminated(ex);
            throw;
        }
    }
}

/// <summary>Source-generated log messages for <see cref="WallabyBackgroundService"/>.</summary>
internal static partial class WallabyBackgroundServiceLog
{
    [LoggerMessage(Level = LogLevel.Critical, Message = "Wallaby background service terminated unexpectedly.")]
    internal static partial void BackgroundServiceTerminated(this ILogger logger, Exception ex);
}
