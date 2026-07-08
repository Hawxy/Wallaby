using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using NpgsqlTypes;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Internal;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;

namespace Wallaby.Hosting;

/// <summary>
/// One leadership term: self-configure, repair any slot-loss gap, initialize sinks, then run the live
/// pipeline alongside the backfill scheduler and fan-out worker until shutdown, a lost lock, or a fault.
/// Owns the term's resources (the spill, replication stream, and background tasks); the long-lived
/// components come from <see cref="WallabyComponents"/>. Constructed per leadership acquisition by
/// <see cref="WallabyRuntime"/>'s election loop.
/// </summary>
internal sealed class LeaderSession(
    WallabyComponents components,
    WallabyConfiguration config,
    WallabyOptions options,
    WallabyDataSource dataSource,
    IClusterLockHandle leadership,
    IServiceProvider services,
    WallabyInstrumentation instrumentation,
    WallabyStatus status,
    ILogger logger)
{
    /// <summary>
    /// Run the leader workload for the lifetime of leadership. Returns true if it ended because the
    /// cluster lock was lost (so the caller re-elects without treating it as a fault); a real fault —
    /// from the pipeline or a background task — propagates, and shutdown re-throws cancellation.
    /// </summary>
    public async Task<bool> RunAsync(CancellationToken ct)
    {
        await BootstrapAsync(ct);

        // Spill target for pgoutput v2 streamed (large) transactions. Clear any leftovers from a prior crash —
        // an un-acked streamed transaction is re-streamed from the slot, so stale spill data is never needed.
        await using var spill = CreateSpill();
        await spill.ClearAsync(ct);

        await using var stream = new LogicalReplicationStream(
            dataSource.ConnectionString, options.SlotName, options.PublicationName, spill,
            options.Advanced.MaxBufferedChangesPerTransaction);
        var changeEventFactory = new ChangeEventFactory(components.Materializer);
        var pipeline = new WallabyPipeline(
            stream, changeEventFactory, components.Router, components.Dispatcher, components.Checkpoints,
            options.SlotName, logger, options.MaxBatchSize, options.Advanced.KeepaliveInterval, components.Coordinator,
            components.DependentResolver, components.FanoutQueue, instrumentation, status);

        // Cancel the whole leader workload on shutdown OR when the handle reports the lock was lost (its
        // connection dropped) so a standby that can take over isn't left waiting while we stream on with
        // a stale lock.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, leadership.Lost);
        var scheduler = new BackfillScheduler(
            components.BackfillTables, components.BackfillStore, components.Coordinator,
            new BackfillSchedulerOptions
            {
                AutoBackfillNewTables = options.AutoBackfillNewTables,
                AutoBackfillOnVersionChange = options.AutoBackfillOnVersionChange,
            },
            logger);

        // A background-task fault fails the whole leader session (first fault wins): the task records it,
        // cancels the workload, and the fault is rethrown below so the caller halts and retries with backoff.
        Exception? backgroundFault = null;

        var backfillTask = Task.Run(async () =>
        {
            try { await scheduler.RunDueBackfillsAsync(linked.Token); }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                logger.BackfillSchedulerFailed(ex);
                Interlocked.CompareExchange(ref backgroundFault, ex, null);
                await linked.CancelAsync();
            }
        });

        // The fan-out worker drains offloaded scoped re-snapshots for the lifetime of leadership.
        var fanoutTask = components.FanoutQueue is not null
            ? Task.Run(async () =>
            {
                try { await new FanoutQueueWorker(components.FanoutQueue, components.Coordinator, components.Model, logger, options.Advanced.FanoutPollInterval, status, instrumentation).RunAsync(linked.Token); }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    logger.FanoutWorkerFailed(ex);
                    Interlocked.CompareExchange(ref backgroundFault, ex, null);
                    await linked.CancelAsync();
                }
            })
            : Task.CompletedTask;

        try
        {
            await pipeline.RunAsync(linked.Token);
        }
        catch (OperationCanceledException)
        {
            // Shutdown (ct), lost-lock, or a background fault cancelled the workload — distinguished below.
        }
        finally
        {
            await linked.CancelAsync();
            await backfillTask; // never faults: the body records + swallows
            await fanoutTask;
        }

        ct.ThrowIfCancellationRequested();        // a real shutdown re-throws so the caller's loop breaks
        if (backgroundFault is not null)
        {
            ExceptionDispatchInfo.Capture(backgroundFault).Throw(); // fail the session so the caller retries with backoff
        }
        return leadership.Lost.IsCancellationRequested; // otherwise: did we step down because the lock dropped?
    }

    // Self-configure, repair any slot-loss gap, and initialize sinks — grouped under one bootstrap span so
    // a slow startup (slot creation, index setup) is visible as a single trace per leadership term.
    private async Task BootstrapAsync(CancellationToken ct)
    {
        using var bootstrap = instrumentation.StartLeaderBootstrap();
        bootstrap?.SetTag(WallabyInstrumentation.SlotTag, options.SlotName);
        try
        {
            SelfConfigResult selfConfig;
            using (instrumentation.StartSelfConfig())
            {
                selfConfig = await components.SelfConfigurator.EnsureConfiguredAsync(components.Model, ct);
            }
            using (instrumentation.StartSlotRepair())
            {
                await RepairSlotGapAsync(selfConfig, ct);
            }
            await InitializeSinksAsync(ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            bootstrap?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Detects and repairs a slot-loss gap: a persisted checkpoint behind the slot's consistent point can
    /// only mean the slot was recreated after that checkpoint was written (invalidation, failover, manual
    /// drop), so every change between the two LSNs was never streamed. Repairs by marking all mapped
    /// tables for re-backfill; the marks are durable before the checkpoint advances to the consistent
    /// point, so a crash mid-repair re-detects on the next leader session.
    /// </summary>
    private async Task RepairSlotGapAsync(SelfConfigResult selfConfig, CancellationToken ct)
    {
        var consistentPoint = selfConfig.ConsistentPoint ?? await ReadRegisteredConsistentPointAsync(ct);
        if (consistentPoint is null)
        {
            return;
        }

        var checkpoint = await components.CheckpointsDirect.GetAsync(options.SlotName, ct);
        var consistentLsn = ParseLsn(consistentPoint);
        if (checkpoint is null || checkpoint.ConfirmedLsn >= consistentLsn)
        {
            return;
        }

        logger.SlotGapDetected(
            options.SlotName, new NpgsqlLogSequenceNumber(checkpoint.ConfirmedLsn).ToString(), consistentPoint);

        foreach (var (table, _) in components.BackfillTables)
        {
            var existing = await components.BackfillStore.GetAsync(table.QualifiedName, ct);
            await components.BackfillStore.SaveAsync(
                new BackfillState(
                    table.QualifiedName, BackfillStatus.Requested, existing?.TransformVersion,
                    null, 0, DateTimeOffset.UtcNow),
                ct);
        }

        await components.CheckpointsDirect.SaveAsync(
            options.SlotName, new Checkpoint(consistentLsn, DateTimeOffset.UtcNow), ct);
    }

    private async Task<string?> ReadRegisteredConsistentPointAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.Source.OpenConnectionAsync(ct);
        return await PgExec.ScalarStringAsync(
            connection,
            "SELECT consistent_point::text FROM wallaby.slot_registry WHERE slot_name = @s", ct,
            ("s", options.SlotName));
    }

    // pg_lsn text form: two hex words separated by '/'.
    private static ulong ParseLsn(string lsn)
    {
        var slash = lsn.IndexOf('/');
        var hi = ulong.Parse(lsn.AsSpan(0, slash), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        var lo = ulong.Parse(lsn.AsSpan(slash + 1), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        return (hi << 32) | lo;
    }

    // Runs each sink's optional one-time setup on the leader, before streaming. Idempotent, so it is safe
    // to re-run on every leadership acquisition; a failure bubbles to the leader retry loop.
    private async Task InitializeSinksAsync(CancellationToken ct)
    {
        foreach (var sink in components.Sinks.Values)
        {
            if (sink is ISinkInitializer initializer)
            {
                using var activity = instrumentation.StartSinkInitialize();
                activity?.SetTag(WallabyInstrumentation.SinkTag, sink.Name);
                await initializer.InitializeAsync(ct);
                logger.SinkInitialized(sink.Name);
            }
        }
    }

    // The configured spill factory builds the backend for this leader session; default = the database-backed spill.
    private ITransactionSpill CreateSpill()
    {
        var factory = config.SpillFactory ?? DefaultSpill;
        return factory(new SpillContext(dataSource.Source, options.SlotName, services));
    }

    private static ITransactionSpill DefaultSpill(SpillContext ctx)
        => new PostgresUnloggedTableSpill(ctx.DataSource, ctx.SlotName);
}

/// <summary>Source-generated log messages for <see cref="LeaderSession"/>.</summary>
internal static partial class LeaderSessionLog
{
    [LoggerMessage(Level = LogLevel.Error, Message = "Replication slot '{Slot}' was recreated: changes between {CheckpointLsn} and {ConsistentPoint} were never streamed. Re-backfilling all mapped tables to converge sinks.")]
    internal static partial void SlotGapDetected(this ILogger logger, string slot, string checkpointLsn, string consistentPoint);

    [LoggerMessage(Level = LogLevel.Error, Message = "Backfill scheduler failed.")]
    internal static partial void BackfillSchedulerFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Fan-out queue worker failed.")]
    internal static partial void FanoutWorkerFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Initialized sink '{Sink}'.")]
    internal static partial void SinkInitialized(this ILogger logger, string sink);
}
