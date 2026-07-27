using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Disposes the sinks a pipeline materialized. Disposal is an opt-in sink capability: a sink implementing
/// <see cref="IAsyncDisposable"/> (preferred) or <see cref="IDisposable"/> is disposed once at shutdown.
/// </summary>
internal static class SinkDisposal
{
    /// <summary>
    /// Dispose every opted-in sink. Never throws: a failing dispose is logged and the remaining sinks are
    /// still disposed, since this runs on shutdown and fault-unwind paths.
    /// </summary>
    public static async ValueTask DisposeAllAsync(IEnumerable<ISink> sinks, ILogger logger)
    {
        foreach (var sink in sinks)
        {
            try
            {
                if (sink is IAsyncDisposable asyncDisposable)
                {
                    await asyncDisposable.DisposeAsync();
                }
                else if (sink is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            catch (Exception ex)
            {
                logger.SinkDisposeFailed(ex, sink.Name);
            }
        }
    }
}

/// <summary>Source-generated log messages for <see cref="SinkDisposal"/>.</summary>
internal static partial class SinkDisposalLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Sink {Sink} threw while being disposed; continuing shutdown.")]
    internal static partial void SinkDisposeFailed(this ILogger logger, Exception ex, string sink);
}
