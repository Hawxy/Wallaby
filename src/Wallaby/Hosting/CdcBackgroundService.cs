using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Wallaby.Hosting;

/// <summary>Hosts the <see cref="CdcRuntime{TContext}"/> as a long-running background service.</summary>
internal sealed class CdcBackgroundService<TContext>(
    CdcRuntime<TContext> runtime, ILogger<CdcBackgroundService<TContext>> logger) : BackgroundService
    where TContext : DbContext
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await runtime.RunAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
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
