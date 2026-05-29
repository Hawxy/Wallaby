using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EFCore.CDC.Hosting;

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
            logger.LogCritical(ex, "CDC background service terminated unexpectedly.");
            throw;
        }
    }
}
