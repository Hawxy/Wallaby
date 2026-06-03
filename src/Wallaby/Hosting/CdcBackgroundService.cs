using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wallaby.Diagnostics;

namespace Wallaby.Hosting;

/// <summary>Hosts the <see cref="CdcRuntime{TContext}"/> as a long-running background service.</summary>
internal sealed class CdcBackgroundService<TContext>(
    CdcRuntime<TContext> runtime, CdcStatus status, ILogger<CdcBackgroundService<TContext>> logger) : BackgroundService
    where TContext : DbContext
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
            // The only reliable "CDC died" signal — BackgroundService exposes no queryable terminated flag.
            status.MarkFaulted($"{ex.GetType().Name}: {ex.Message}");
            logger.BackgroundServiceTerminated(ex);
            throw;
        }
    }
}

/// <summary>Source-generated log messages for <see cref="CdcBackgroundService{TContext}"/>.</summary>
internal static partial class CdcBackgroundServiceLog
{
    [LoggerMessage(Level = LogLevel.Critical, Message = "CDC background service terminated unexpectedly.")]
    internal static partial void BackgroundServiceTerminated(this ILogger logger, Exception ex);
}
