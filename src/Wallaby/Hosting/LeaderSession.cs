using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Internal;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Control;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;

namespace Wallaby.Hosting;

/// <summary>Why a leader session ended (faults propagate as exceptions instead).</summary>
internal enum LeaderSessionOutcome
{
    /// <summary>The workload ended without losing the lock or being suspended.</summary>
    Ended,

    /// <summary>The cluster lock was lost (connection dropped); the caller re-elects without treating it as a fault.</summary>
    LeadershipLost,

    /// <summary>
    /// A suspension is in effect or was requested: the session wound down (or never started) without
    /// touching slots, and the caller (still holding the lock) finalizes by dropping them.
    /// </summary>
    SuspendRequested,
}

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
    WallabyStatus status)
{
    private readonly ILogger _logger = components.Logger;

    /// <summary>
    /// Run the leader workload for the lifetime of leadership. Returns how the workload ended (lost lock,
    /// suspension, or a plain end); a real fault (from the pipeline or a background task) propagates,
    /// and shutdown re-throws cancellation.
    /// </summary>
    public async Task<LeaderSessionOutcome> RunAsync(CancellationToken ct)
    {
        var controlStore = new PostgresControlStore(dataSource, options, _logger);

        // A suspension in force must be honored before self-config can recreate any slot. Tolerates a
        // database no suspension-aware version has touched (no control table reads as running).
        if (await controlStore.IsSuspensionInEffectAsync(ct))
        {
            return LeaderSessionOutcome.SuspendRequested;
        }

        // Cancel the whole leader workload on shutdown OR when the handle reports the lock was lost (its
        // connection dropped) so a standby that can take over isn't left waiting while we stream on with
        // a stale lock. Bootstrap runs under the same token: an ex-leader must not keep issuing DDL.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, leadership.Lost);

        try
        {
            await BootstrapAsync(linked.Token);
        }
        catch (OperationCanceledException) when (leadership.Lost.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            // A lock loss during bootstrap is a normal step-down, not a fault.
            return LeaderSessionOutcome.LeadershipLost;
        }

        // Spill target for pgoutput v2 streamed (large) transactions. Leftovers from a prior crash are
        // cleared by the stream once it exclusively holds the slot.
        await using var spill = CreateSpill();

        // Npgsql rejects a multi-host replication connection, so a multi-host string is resolved to its
        // primary by probing; re-resolved each session, so a failover is picked up on re-election.
        var replicationConnectionString = await ReplicationPrimaryResolver.ResolveAsync(
            dataSource.ConnectionString, linked.Token);
        if (!ReferenceEquals(replicationConnectionString, dataSource.ConnectionString))
        {
            _logger.ReplicationPrimaryResolved(new NpgsqlConnectionStringBuilder(replicationConnectionString).Host!);
        }

        await using var stream = new LogicalReplicationStream(
            replicationConnectionString, options.SlotName, options.PublicationName, spill,
            options.Advanced.MaxBufferedChangesPerTransaction, components.Model);
        var changeEventFactory = new ChangeEventFactory(
            components.Materializer, components.Reselector, _logger, instrumentation);
        var pipeline = new WallabyPipeline(
            stream, changeEventFactory, components.Router, components.Dispatcher, components.Checkpoints,
            options.SlotName, _logger, options.MaxBatchSize, options.Advanced.KeepaliveInterval, components.Coordinator,
            components.DependentResolver, components.FanoutQueue, instrumentation, status,
            options.Advanced.MaxTransactionsPerBatch, components.BackfillStore);

        var scheduler = new BackfillScheduler(
            components.BackfillTables, components.BackfillStore, components.Coordinator,
            new SinkPurgeRunner(components.Sinks, instrumentation, _logger),
            new BackfillSchedulerOptions
            {
                AutoBackfillNewTables = options.AutoBackfillNewTables,
                AutoBackfillOnVersionChange = options.AutoBackfillOnVersionChange,
            },
            _logger);

        // A background-task fault fails the whole leader session (first fault wins): the task records it,
        // cancels the workload, and the fault is rethrown below so the caller halts and retries with backoff.
        Exception? backgroundFault = null;

        var backfillTask = Task.Run(async () =>
        {
            try { await scheduler.RunAsync(options.Advanced.BackfillPollInterval, linked.Token); }
            catch (OperationCanceledException) when (linked.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _logger.BackfillSchedulerFailed(ex);
                Interlocked.CompareExchange(ref backgroundFault, ex, null);
                await linked.CancelAsync();
            }
        });

        // Watches for a suspension request (LISTEN + fallback poll) and cancels the workload so the
        // session winds down and releases the slot; the caller then drops it. Its first read also closes
        // the race where a suspension lands between this session's pre-check and slot creation. Never
        // faults the session; transient read errors are retried inside.
        var controlWatcher = new ControlStateWatcher(controlStore, options.Advanced.ControlPollInterval, _logger);
        var controlTask = Task.Run(async () =>
        {
            try { await controlWatcher.RunAsync(linked, linked.Token); }
            catch (OperationCanceledException) when (linked.IsCancellationRequested) { }
        });

        // Advances the slot while the mapped tables are idle. Never faults the session: the emitter
        // logs and swallows per-tick errors, so a transiently-down database just skips ticks.
        var heartbeatTask = options.Advanced.HeartbeatInterval > TimeSpan.Zero
            ? Task.Run(async () =>
            {
                var emitter = new HeartbeatEmitter(
                    dataSource.Source, () => pipeline.LastAcknowledgedLsn,
                    options.Advanced.HeartbeatInterval, _logger);
                try { await emitter.RunAsync(linked.Token); }
                catch (OperationCanceledException) when (linked.IsCancellationRequested) { }
            })
            : Task.CompletedTask;

        // Publishes the retained-WAL gauge while leading. Never faults the session: the sampler logs
        // and swallows per-tick errors.
        var slotLagTask = options.Advanced.SlotLagSampleInterval > TimeSpan.Zero
            ? Task.Run(async () =>
            {
                var sampler = new SlotLagSampler(
                    dataSource.Source, options.SlotName, options.Advanced.SlotLagSampleInterval,
                    instrumentation, _logger);
                try { await sampler.RunAsync(linked.Token); }
                catch (OperationCanceledException) when (linked.IsCancellationRequested) { }
            })
            : Task.CompletedTask;

        // The fan-out worker drains offloaded scoped re-snapshots for the lifetime of leadership.
        var fanoutTask = components.FanoutQueue is not null
            ? Task.Run(async () =>
            {
                try { await new FanoutQueueWorker(components.FanoutQueue, components.Coordinator, components.Model, _logger, options.Advanced.FanoutPollInterval, status, instrumentation).RunAsync(linked.Token); }
                catch (OperationCanceledException) when (linked.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    _logger.FanoutWorkerFailed(ex);
                    Interlocked.CompareExchange(ref backgroundFault, ex, null);
                    await linked.CancelAsync();
                }
            })
            : Task.CompletedTask;

        try
        {
            await pipeline.RunAsync(linked.Token);
        }
        catch (OperationCanceledException) when (linked.IsCancellationRequested)
        {
            // Shutdown (ct), lost-lock, or a background fault cancelled the workload, distinguished below.
            // An OCE while the workload is NOT being cancelled (e.g. a sink-thrown TaskCanceledException
            // from an HTTP timeout) is a real fault and propagates like any other exception.
        }
        finally
        {
            await linked.CancelAsync();
            await backfillTask; // never faults: the body records + swallows
            await fanoutTask;
            await controlTask;
            await heartbeatTask;
            await slotLagTask;
        }

        ct.ThrowIfCancellationRequested();        // a real shutdown re-throws so the caller's loop breaks
        if (controlWatcher.SuspendObserved)
        {
            // The stream's await using scope disposes on method exit, so by the time the caller sees this
            // outcome the slot is released and free to drop while the cluster lock is still held.
            return LeaderSessionOutcome.SuspendRequested;
        }
        if (backgroundFault is not null)
        {
            ExceptionDispatchInfo.Capture(backgroundFault).Throw(); // fail the session so the caller retries with backoff
        }
        return leadership.Lost.IsCancellationRequested // otherwise: did we step down because the lock dropped?
            ? LeaderSessionOutcome.LeadershipLost
            : LeaderSessionOutcome.Ended;
    }

    // Self-configure, repair any slot-loss gap, and initialize sinks, grouped under one bootstrap span so
    // a slow startup (slot creation, index setup) is visible as a single trace per leadership term.
    private async Task BootstrapAsync(CancellationToken ct)
    {
        using var bootstrap = instrumentation.StartLeaderBootstrap();
        bootstrap?.SetTag(WallabyInstrumentation.SlotTag, options.SlotName);
        try
        {
            var selfConfig = await components.SelfConfigurator.EnsureConfiguredAsync(components.Model, ct);
            await RepairSlotGapAsync(selfConfig, ct);
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
    /// drop), so every change between the two LSNs was never streamed. A recreated slot with no
    /// checkpoint at all is the same gap observed before the first interval-throttled checkpoint save
    /// (e.g. suspend/resume early in an install's life, or while it only ever backfilled), so it
    /// repairs too. Repairs by marking all mapped tables for re-backfill; the marks are durable before
    /// the checkpoint advances to the consistent point, so a crash mid-repair re-detects on the next
    /// leader session.
    /// </summary>
    private async Task RepairSlotGapAsync(SelfConfigResult selfConfig, CancellationToken ct)
    {
        using var repair = instrumentation.StartSlotRepair();
        var consistentPoint = selfConfig.ConsistentPoint ?? await ReadRegisteredConsistentPointAsync(ct);
        if (consistentPoint is null)
        {
            return;
        }

        var checkpoint = await components.CheckpointsDirect.GetAsync(options.SlotName, ct);
        var consistentLsn = ParseLsn(consistentPoint);
        if (checkpoint is null)
        {
            // No checkpoint has ever been written for this slot. If the slot is a recreation, the
            // installation may have delivered before the slot was dropped; nothing proves continuity,
            // so repair. A first-ever slot (no prior registry row) has missed nothing.
            if (!selfConfig.SlotRecreated)
            {
                return;
            }
            _logger.SlotRecreatedBeforeFirstCheckpoint(options.SlotName, consistentPoint);
        }
        else if (checkpoint.ConfirmedLsn >= consistentLsn)
        {
            return;
        }
        else
        {
            _logger.SlotGapDetected(
                options.SlotName, new NpgsqlLogSequenceNumber(checkpoint.ConfirmedLsn).ToString(), consistentPoint);
        }

        repair?.AddEvent(new ActivityEvent("slot.gap", tags: new ActivityTagsCollection
        {
            ["wallaby.lsn.checkpoint"] = checkpoint is null
                ? "none"
                : new NpgsqlLogSequenceNumber(checkpoint.ConfirmedLsn).ToString(),
            ["wallaby.lsn.consistent"] = consistentPoint,
        }));

        foreach (var table in components.BackfillTables)
        {
            var existing = await components.BackfillStore.GetAsync(table.Table.QualifiedName, ct);
            await components.BackfillStore.SaveAsync(
                new BackfillState(
                    table.Table.QualifiedName, BackfillStatus.Requested, existing?.TransformVersion,
                    null, 0, DateTimeOffset.UtcNow, Purge: options.PurgeOnSlotGapRepair),
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
                _logger.SinkInitialized(sink.Name);
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
    [LoggerMessage(Level = LogLevel.Error, Message = "Replication slot {Slot} was recreated: changes between {CheckpointLsn} and {ConsistentPoint} were never streamed. Re-backfilling all mapped tables to converge sinks.")]
    internal static partial void SlotGapDetected(this ILogger logger, string slot, string checkpointLsn, string consistentPoint);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Replication slot {Slot} was recreated (at {ConsistentPoint}) before its first checkpoint was written: changes committed while the slot was gone were never streamed. Re-backfilling all mapped tables to converge sinks.")]
    internal static partial void SlotRecreatedBeforeFirstCheckpoint(this ILogger logger, string slot, string consistentPoint);

    [LoggerMessage(Level = LogLevel.Information, Message = "Resolved {Host} as the primary for the replication connection (multi-host connection string).")]
    internal static partial void ReplicationPrimaryResolved(this ILogger logger, string host);

    [LoggerMessage(Level = LogLevel.Error, Message = "Backfill scheduler failed.")]
    internal static partial void BackfillSchedulerFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Fan-out queue worker failed.")]
    internal static partial void FanoutWorkerFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Initialized sink {Sink}.")]
    internal static partial void SinkInitialized(this ILogger logger, string sink);
}
