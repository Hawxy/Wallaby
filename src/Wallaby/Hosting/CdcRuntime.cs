using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.Logging;
using NpgsqlTypes;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Internal;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Materialization;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Hosting;

/// <summary>
/// Owns the end-to-end Wallaby lifecycle for a context: elect leadership (cluster lock), self-configure,
/// then run the live pipeline and backfill scheduler. Standby nodes wait and take over on failover.
/// </summary>
internal sealed class CdcRuntime
{
    private readonly CapturedModel _capturedModel;
    private readonly CdcConfiguration _config;
    private readonly WallabyOptions _options;
    private readonly CdcDataSource _dataSource;
    private readonly IClusterLock _clusterLock;
    private readonly IServiceProvider _services;
    private readonly WallabyInstrumentation _instrumentation;
    private readonly CdcStatus _status;
    private readonly ILogger<CdcRuntime> _logger;

    // Built once.
    private WallabyModel _cdcModel = null!;
    private EntityMaterializer _materializer = null!;
    private MappingChangeRouter _router = null!;
    private SinkDispatcher _dispatcher = null!;
    private IReadOnlyDictionary<string, ISink> _sinks = null!;
    private WatermarkBackfillCoordinator _coordinator = null!;
    private PostgresSelfConfigurator _selfConfigurator = null!;
    private ICheckpointStore _checkpoints = null!;
    private PostgresCheckpointStore _checkpointsDirect = null!;
    private DependentChangeResolver? _dependentResolver;
    private IFanoutQueueStore? _fanoutQueue;
    private IReadOnlyList<(CapturedTable Table, string? Version)> _backfillTables = [];

    public CdcRuntime(
        CapturedModel capturedModel,
        CdcConfiguration config,
        WallabyOptions options,
        CdcDataSource dataSource,
        IClusterLock clusterLock,
        IServiceProvider services,
        WallabyInstrumentation instrumentation,
        CdcStatus status,
        ILogger<CdcRuntime> logger)
    {
        _capturedModel = capturedModel;
        _config = config;
        _options = options;
        _dataSource = dataSource;
        _clusterLock = clusterLock;
        _services = services;
        _instrumentation = instrumentation;
        _status = status;
        _logger = logger;
    }

    // A leader session lasting at least this long before failing retries at the base delay (resets backoff);
    // a faster failure (e.g. self-config erroring) grows the backoff so it doesn't hot-loop.
    private static readonly TimeSpan HealthyLeaderSession = TimeSpan.FromMinutes(1);

    public async Task RunAsync(CancellationToken ct)
    {
        BuildComponents();

        // Grows the retry delay (with jitter, capped) when leadership acquisition or a leader session keeps
        // failing, so a persistent error backs off instead of spamming at a fixed interval.
        var backoff = new RetryBackoff(_options.LeaderRetryInterval);

        while (!ct.IsCancellationRequested)
        {
            IClusterLockHandle? leadership = null;
            try
            {
                leadership = await _clusterLock.TryAcquireAsync(_options.SlotName, ct);
            }
            catch (Exception ex)
            {
                _logger.LeadershipAcquireFailed(ex);
                await DelaySafeAsync(backoff.Next(), ct);
                continue;
            }

            if (leadership is null)
            {
                // The lock is reachable and held by another node — a healthy standby, not an error.
                backoff.Reset();
                _status.EnterStandby();
                _status.ResetLeaderFailures();
                _status.ResetFanoutFailures();
                _logger.Standby(_options.SlotName);
                await DelaySafeAsync(_options.StandbyRetryInterval, ct);
                continue;
            }

            await using (leadership)
            {
                _logger.LeadershipAcquired(_options.SlotName);
                _status.EnterLeader(DateTimeOffset.UtcNow);
                var sessionStart = Stopwatch.GetTimestamp();
                try
                {
                    var lostLeadership = await RunAsLeaderAsync(leadership, ct);
                    backoff.Reset();
                    _status.ResetLeaderFailures();
                    _status.ResetFanoutFailures();
                    if (lostLeadership)
                    {
                        // We stepped down because the lock dropped (not an error); re-elect immediately.
                        _logger.LeadershipLost(_options.SlotName);
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LeaderSessionFailed(ex);
                    _status.RecordLeaderFailure(Describe(ex));
                    if (Stopwatch.GetElapsedTime(sessionStart) >= HealthyLeaderSession)
                    {
                        // A long session that then dropped is likely transient — retry at the base delay.
                        // The failure counter clears only on real progress or a clean step-down.
                        backoff.Reset();
                    }
                    await DelaySafeAsync(backoff.Next(), ct);
                }
            }
        }
    }

    /// <summary>
    /// Run the leader workload (self-config, sinks, pipeline, backfill/fan-out) for the lifetime of
    /// leadership. Returns true if it ended because the cluster lock was lost (so the caller re-elects
    /// without treating it as a fault); a real fault — from the pipeline or a background task — propagates,
    /// and shutdown re-throws cancellation.
    /// </summary>
    private async Task<bool> RunAsLeaderAsync(IClusterLockHandle leadership, CancellationToken ct)
    {
        var selfConfig = await _selfConfigurator.EnsureConfiguredAsync(_cdcModel, ct);
        await RepairSlotGapAsync(selfConfig, ct);
        await InitializeSinksAsync(ct);

        // Spill target for pgoutput v2 streamed (large) transactions. Clear any leftovers from a prior crash —
        // an un-acked streamed transaction is re-streamed from the slot, so stale spill data is never needed.
        await using var spill = CreateSpill();
        await spill.ClearAsync(ct);

        await using var stream = new LogicalReplicationStream(
            _dataSource.ConnectionString, _options.SlotName, _options.PublicationName, spill,
            _options.MaxBufferedChangesPerTransaction);
        var changeEventFactory = new ChangeEventFactory(_materializer);
        var pipeline = new CdcPipeline(
            stream, changeEventFactory, _router, _dispatcher, _checkpoints, _options.SlotName, _logger,
            _options.MaxBatchSize, _options.KeepaliveInterval, _coordinator, _dependentResolver, _fanoutQueue,
            _instrumentation, _status);

        // Cancel the whole leader workload on shutdown OR when the handle reports the lock was lost (its
        // connection dropped) so a standby that can take over isn't left waiting while we stream on with
        // a stale lock.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, leadership.Lost);
        var scheduler = new BackfillScheduler(
            _backfillTables, new PostgresBackfillStore(_dataSource.Source), _coordinator,
            new BackfillSchedulerOptions
            {
                AutoBackfillNewTables = _options.AutoBackfillNewTables,
                AutoBackfillOnVersionChange = _options.AutoBackfillOnVersionChange,
            },
            _logger);

        // A background-task fault fails the whole leader session (first fault wins): the task records it,
        // cancels the workload, and the fault is rethrown below so the caller halts and retries with backoff.
        Exception? backgroundFault = null;

        var backfillTask = Task.Run(async () =>
        {
            try { await scheduler.RunDueBackfillsAsync(linked.Token); }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.BackfillSchedulerFailed(ex);
                Interlocked.CompareExchange(ref backgroundFault, ex, null);
                await linked.CancelAsync();
            }
        });

        // The fan-out worker drains offloaded scoped re-snapshots for the lifetime of leadership.
        var fanoutTask = _fanoutQueue is not null
            ? Task.Run(async () =>
            {
                try { await new FanoutQueueWorker(_fanoutQueue, _coordinator, _cdcModel, _logger, _options.FanoutPollInterval, _status, _instrumentation).RunAsync(linked.Token); }
                catch (OperationCanceledException) { }
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

        var checkpoint = await _checkpointsDirect.GetAsync(_options.SlotName, ct);
        var consistentLsn = ParseLsn(consistentPoint);
        if (checkpoint is null || checkpoint.ConfirmedLsn >= consistentLsn)
        {
            return;
        }

        _logger.SlotGapDetected(
            _options.SlotName, new NpgsqlLogSequenceNumber(checkpoint.ConfirmedLsn).ToString(), consistentPoint);

        var store = new PostgresBackfillStore(_dataSource.Source);
        foreach (var (table, _) in _backfillTables)
        {
            var existing = await store.GetAsync(table.QualifiedName, ct);
            await store.SaveAsync(
                new BackfillState(
                    table.QualifiedName, BackfillStatus.Requested, existing?.TransformVersion,
                    null, 0, DateTimeOffset.UtcNow),
                ct);
        }

        await _checkpointsDirect.SaveAsync(
            _options.SlotName, new Checkpoint(consistentLsn, DateTimeOffset.UtcNow), ct);
    }

    private async Task<string?> ReadRegisteredConsistentPointAsync(CancellationToken ct)
    {
        await using var connection = await _dataSource.Source.OpenConnectionAsync(ct);
        return await PgExec.ScalarStringAsync(
            connection,
            "SELECT consistent_point::text FROM wallaby.slot_registry WHERE slot_name = @s", ct,
            ("s", _options.SlotName));
    }

    // pg_lsn text form: two hex words separated by '/'.
    private static ulong ParseLsn(string lsn)
    {
        var slash = lsn.IndexOf('/');
        var hi = ulong.Parse(lsn.AsSpan(0, slash), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        var lo = ulong.Parse(lsn.AsSpan(slash + 1), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        return (hi << 32) | lo;
    }

    private void BuildComponents()
    {
        _cdcModel = _capturedModel.Cdc;
        _materializer = new EntityMaterializer(_capturedModel.EfModel);

        var mappings = new Dictionary<Type, EntityMapping>();
        var backfillTables = new List<(CapturedTable, string?)>();
        foreach (var registration in _config.Mappings.Values)
        {
            var captured = _cdcModel.FindByClrType(registration.EntityClrType)
                ?? throw new WallabyConfigurationException(
                    $"Mapped entity '{registration.EntityClrType.FullName}' is not captured. Ensure it is declared and mapped to a table.");

            mappings[registration.EntityClrType] = new EntityMapping
            {
                EntityClrType = registration.EntityClrType,
                SinkName = registration.SinkName,
                Destination = registration.Destination,
                BackfillVersion = registration.BackfillVersion,
                Transform = registration.TransformFactory!(_services),
                DocumentIdSelector = registration.DocumentIdSelector,
                ScopeKeySelector = registration.ScopeKeySelector,
                DestinationSelector = registration.DestinationSelector,
            };
            backfillTables.Add((captured, registration.BackfillVersion));
        }

        _sinks = _config.Sinks.ToDictionary(s => s.Name, s => s.Factory(_services));

        IEnrichmentContextProvider contextProvider = _config.ScopedContextFactory is { } scopedFactory
            ? new ScopedEnrichmentContextProvider(scopedFactory, _services)
            : new DefaultEnrichmentContextProvider(() => _config.ContextLease!(_services));
        _router = new MappingChangeRouter(mappings, contextProvider, _instrumentation);
        _dispatcher = new SinkDispatcher(_sinks, _instrumentation, _options.SinkRetry, _status);
        _coordinator = new WatermarkBackfillCoordinator(
            _dataSource.Source, new PostgresBackfillStore(_dataSource.Source), _logger, _instrumentation) { ChunkSize = _options.ChunkSize };
        _dependentResolver = _cdcModel.DependentBindings.Count > 0
            ? new DependentChangeResolver(_dataSource.Source, _cdcModel, _instrumentation)
            : null;
        _fanoutQueue = _dependentResolver is not null ? new PostgresFanoutQueueStore(_dataSource.Source) : null;
        _checkpointsDirect = new PostgresCheckpointStore(_dataSource.Source);
        _checkpoints = _options.CheckpointSaveInterval > TimeSpan.Zero
            ? new ThrottledCheckpointStore(_checkpointsDirect, _options.CheckpointSaveInterval)
            : _checkpointsDirect;
        _selfConfigurator = new PostgresSelfConfigurator(
            _dataSource.Source,
            new SelfConfigOptions
            {
                SlotName = _options.SlotName,
                PublicationName = _options.PublicationName,
                ManagePublicationTables = _options.ManagePublicationTables,
                RequireFullReplicaIdentity = _options.RequireFullReplicaIdentity,
                ExternalSlots = ExternalSlotResolver.Resolve(_config.ExternalSlots, _capturedModel.EfModel),
            },
            _logger);
        _backfillTables = backfillTables;
    }

    // Runs each sink's optional one-time setup on the leader, before streaming. Idempotent, so it is safe
    // to re-run on every leadership acquisition; a failure bubbles to the leader retry loop in RunAsync.
    private async Task InitializeSinksAsync(CancellationToken ct)
    {
        foreach (var sink in _sinks.Values)
        {
            if (sink is ISinkInitializer initializer)
            {
                await initializer.InitializeAsync(ct);
                _logger.SinkInitialized(sink.Name);
            }
        }
    }

    private async Task DelaySafeAsync(TimeSpan delay, CancellationToken ct)
    {
        try { await Task.Delay(delay, ct); }
        catch (OperationCanceledException) { }
    }

    // The configured spill factory builds the backend for this leader session; default = the database-backed spill.
    private ITransactionSpill CreateSpill()
    {
        var factory = _config.SpillFactory ?? DefaultSpill;
        return factory(new SpillContext(_dataSource.Source, _options.SlotName, _services));
    }

    private static ITransactionSpill DefaultSpill(SpillContext ctx)
        => new PostgresUnloggedTableSpill(ctx.DataSource, ctx.SlotName);

    private static string Describe(Exception ex) => $"{ex.GetType().Name}: {ex.Message}";
}

/// <summary>Source-generated log messages for <see cref="CdcRuntime"/>.</summary>
internal static partial class CdcRuntimeLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to acquire Wallaby leadership; retrying.")]
    internal static partial void LeadershipAcquireFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Wallaby standby: another node holds leadership for slot '{Slot}'.")]
    internal static partial void Standby(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Information, Message = "Acquired Wallaby leadership for slot '{Slot}'.")]
    internal static partial void LeadershipAcquired(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Lost Wallaby leadership for slot '{Slot}' (lock connection dropped); stepping down and re-electing.")]
    internal static partial void LeadershipLost(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Error, Message = "Wallaby leader session failed; will retry.")]
    internal static partial void LeaderSessionFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Replication slot '{Slot}' was recreated: changes between {CheckpointLsn} and {ConsistentPoint} were never streamed. Re-backfilling all mapped tables to converge sinks.")]
    internal static partial void SlotGapDetected(this ILogger logger, string slot, string checkpointLsn, string consistentPoint);

    [LoggerMessage(Level = LogLevel.Error, Message = "Backfill scheduler failed.")]
    internal static partial void BackfillSchedulerFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Fan-out queue worker failed.")]
    internal static partial void FanoutWorkerFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Initialized sink '{Sink}'.")]
    internal static partial void SinkInitialized(this ILogger logger, string sink);
}
