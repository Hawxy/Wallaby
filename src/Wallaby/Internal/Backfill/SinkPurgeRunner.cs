using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// Empties a table's sink destinations before a fresh backfill so the backfill converges them to
/// exactly the current table contents. Sinks without <see cref="ISinkPurger"/> and scoped (per-record)
/// destinations are skipped with a warning; a purge failure propagates and fails the backfill run.
/// </summary>
internal sealed class SinkPurgeRunner(
    IReadOnlyDictionary<string, ISink> sinks,
    WallabyInstrumentation instrumentation,
    ILogger logger)
{
    public async Task PurgeAsync(BackfillTable table, CancellationToken ct)
    {
        foreach (var target in table.PurgeTargets)
        {
            var sink = sinks[target.SinkName];
            if (sink is not ISinkPurger purger)
            {
                logger.SinkPurgeUnsupported(sink.Name, table.Table.QualifiedName);
                continue;
            }
            if (target.Scoped)
            {
                logger.SinkPurgeSkippedScopedDestination(sink.Name, table.Table.QualifiedName);
                continue;
            }

            using var activity = instrumentation.StartSinkPurge();
            activity?.SetTag(WallabyInstrumentation.SinkTag, sink.Name);
            activity?.SetTag(WallabyInstrumentation.TableTag, table.Table.QualifiedName);
            if (target.Destination is not null)
            {
                activity?.SetTag(WallabyInstrumentation.DestinationTag, target.Destination);
            }

            try
            {
                await purger.PurgeAsync(
                    new SinkPurgeRequest(table.Table.Schema, table.Table.TableName, target.Destination), ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                throw;
            }
            logger.SinkPurged(sink.Name, table.Table.QualifiedName, target.Destination ?? "(default)");
        }
    }
}

/// <summary>Source-generated log messages for <see cref="SinkPurgeRunner"/>.</summary>
internal static partial class SinkPurgeRunnerLog
{
    [LoggerMessage(Level = LogLevel.Information, Message =
        "Purged sink '{Sink}' destination {Destination} ahead of the fresh backfill of {Table}.")]
    internal static partial void SinkPurged(this ILogger logger, string sink, string table, string destination);

    [LoggerMessage(Level = LogLevel.Warning, Message =
        "A purge was requested for the backfill of {Table}, but sink '{Sink}' does not implement " +
        "ISinkPurger; its destination is not purged and stale documents may remain.")]
    internal static partial void SinkPurgeUnsupported(this ILogger logger, string sink, string table);

    [LoggerMessage(Level = LogLevel.Warning, Message =
        "A purge was requested for the backfill of {Table}, but its mapping to sink '{Sink}' uses a " +
        "scoped destination, which cannot be enumerated; the scoped destinations are not purged and " +
        "stale documents may remain.")]
    internal static partial void SinkPurgeSkippedScopedDestination(this ILogger logger, string sink, string table);
}
