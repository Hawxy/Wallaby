using System.Linq.Expressions;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;
using Wallaby.Model;
using Wallaby.Providers;
using Wallaby.Testing;

namespace Wallaby.TestInfrastructure;

/// <summary>
/// Drives a CDC pipeline against a real Postgres for integration tests, removing the per-test boilerplate
/// of self-config, pipeline wiring, backfill, lifecycle, and fault propagation. Provider-neutral: the
/// storage provider comes in through the constructor's <see cref="IWallabyModelProvider"/>, and provider
/// packages layer their typed configuration on top (e.g. the EF Core <c>ForTestModel</c>/<c>Map</c>/
/// <c>Project</c> extension members in Wallaby.TestInfrastructure.EntityFrameworkCore).
/// </summary>
/// <remarks>
/// Typical one-shot use (with the EF Core test-model extensions):
/// <code>
/// await using var harness = WallabyTestHarness.ForTestModel(conn)
///     .AddSink(sink)
///     .Project&lt;Product&gt;("sink", index, p => new Dictionary&lt;string, object?&gt; { ["name"] = p.Name });
/// await harness.SelfConfigureAsync();
/// await harness.RunUntilAsync(() => /* effect observed */);
/// </code>
/// For backfill or multi-phase tests, use <see cref="StartAsync"/> + <see cref="RunBackfillAsync"/> +
/// <see cref="WaitUntilAsync(Func{Task{bool}}, TimeSpan?)"/> and let <c>await using</c> dispose/stop.
/// </remarks>
public sealed class WallabyTestHarness : IAsyncDisposable
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly IWallabyModelProvider _modelProvider;
    private readonly Dictionary<string, ISink> _sinks = [];
    private readonly List<EntityMapping> _mappings = [];
    private readonly Dictionary<Type, string?> _backfillTypes = [];
    private readonly Dictionary<Type, List<LambdaExpression>> _declaredDependencies = [];
    private readonly Dictionary<Type, List<ColumnSelection>> _columnSelections = [];
    private readonly HashSet<Type> _capturedEntities = [];
    private bool _broadcast;
    private IEnrichmentSessionProvider? _sessionProvider;

    private WallabyModel? _model;
    private IRowMaterializer? _materializer;

    private CancellationTokenSource? _cts;
    private ITransactionSpill? _spill;
    private LogicalReplicationStream? _stream;
    private WallabyPipeline? _pipeline;
    private WatermarkBackfillCoordinator? _coordinator;
    private DependentChangeResolver? _dependentResolver;
    private IFanoutQueueStore? _fanoutQueue;
    private HeartbeatEmitter? _heartbeat;
    private Task? _pipelineTask;
    private Task? _backfillLoopTask;
    private Task? _heartbeatTask;

    public WallabyTestHarness(string connectionString, IWallabyModelProvider modelProvider, WallabyNames? names = null)
    {
        ConnectionString = connectionString;
        _dataSource = NpgsqlDataSource.Create(connectionString);
        _modelProvider = modelProvider;
        Names = names ?? WallabyNames.Unique();
    }

    public string ConnectionString { get; }

    public WallabyNames Names { get; }

    /// <summary>Telemetry holder threaded through the pipeline; attach a <c>MetricCollector</c>/<c>ActivityListener</c> for assertions.</summary>
    public WallabyInstrumentation Instrumentation { get; } = new();

    /// <summary>Backfill keyset page size (set before <see cref="StartAsync"/>).</summary>
    public int ChunkSize { get; set; } = 50;

    /// <summary>Heal unavailable (unchanged TOAST) values by re-reading the row (set before <see cref="StartAsync"/>).</summary>
    public bool ReselectUnavailableValues { get; set; } = true;

    /// <summary>Watermark visibility fence; null = disabled (set before <see cref="StartAsync"/>).</summary>
    internal VisibilityFence? VisibilityFence { get; set; }

    /// <summary>Maximum records per dispatched batch and per inline fan-out page (set before <see cref="StartAsync"/>).</summary>
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>
    /// Maximum committed transactions coalesced into one delivery batch (set before <see cref="StartAsync"/>).
    /// Defaults to the production default; set to 1 for tests that assert per-transaction granularity.
    /// </summary>
    public int MaxTransactionsPerBatch { get; set; } = 100;

    /// <summary>
    /// Safety valve on distinct dependent-lookup keys fanned out per binding per transaction (set before
    /// <see cref="StartAsync"/>). Lower it to exercise the whole-table re-backfill fallback.
    /// </summary>
    public int MaxFanoutKeysPerTransaction { get; set; } = 1_000_000;

    /// <summary>
    /// Distinct lookup keys per offloaded fan-out chunk job (set before <see cref="StartAsync"/>). Lower
    /// it to exercise chunked offload without a huge key set.
    /// </summary>
    public int FanoutChunkSize { get; set; } = 10_000;

    /// <summary>Interval for in-flight replication keepalives during transaction processing (set before <see cref="StartAsync"/>).</summary>
    public TimeSpan KeepaliveInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Idle-slot heartbeat interval (set before <see cref="StartAsync"/>). Opt-in: <see cref="TimeSpan.Zero"/>
    /// (the default) disables the heartbeat so unrelated tests are unaffected.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.Zero;

    /// <summary>Heartbeats emitted since <see cref="StartAsync"/> (for suppression assertions).</summary>
    public long HeartbeatsEmitted => _heartbeat?.EmittedCount ?? 0;

    /// <summary>Sink retry policy (set before <see cref="StartAsync"/>).</summary>
    public DependencyInjection.SinkRetryOptions SinkRetry { get; set; } = new();

    /// <summary>Number of rows currently in the scoped fan-out queue (for coalescing/offload assertions).</summary>
    public async Task<int> PendingFanoutJobCountAsync()
        => _fanoutQueue is null ? 0 : (await _fanoutQueue.ListAsync(_cts?.Token ?? CancellationToken.None)).Count;

    /// <summary>
    /// Empty the shared <c>wallaby.fanout_queue</c> so a test's offload/coalescing assertions are isolated
    /// from rows other tests left in the same database. Call after <see cref="SelfConfigureAsync"/> (which
    /// creates the table) and before any fan-out is triggered.
    /// </summary>
    public async Task ClearFanoutQueueAsync()
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand("DELETE FROM wallaby.fanout_queue", connection);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The highest LSN acknowledged to the server by the running pipeline.</summary>
    public ulong LastAcknowledgedLsn => _pipeline?.LastAcknowledgedLsn ?? 0;

    /// <summary>A backfill manager bound to this harness's database (for manual <c>RequestBackfillAsync</c>).</summary>
    public IWallabyBackfillManager BackfillManager
    {
        get { EnsureModel(); return new DefaultBackfillManager(_model!, new PostgresBackfillStore(_dataSource)); }
    }

    // ---- configuration ----

    public WallabyTestHarness AddSink(ISink sink)
    {
        _sinks[sink.Name] = sink;
        return this;
    }

    /// <summary>Add (and return) a <see cref="CaptureSink"/> that records delivered records for assertions.</summary>
    public CaptureSink AddCaptureSink(string name = "capture")
    {
        var sink = new CaptureSink(name);
        _sinks[name] = sink;
        return sink;
    }

    /// <summary>Route every change to every registered sink (the <see cref="ChangeEvent"/> is the document).</summary>
    public WallabyTestHarness Broadcast()
    {
        _broadcast = true;
        return this;
    }

    /// <summary>
    /// Declare an entity for capture without routing it (mappings declare theirs implicitly). Needed by
    /// <see cref="Broadcast"/>-style tests, which have no mappings to derive the capture set from.
    /// </summary>
    public WallabyTestHarness Capture<TEntity>()
    {
        _capturedEntities.Add(typeof(TEntity));
        return this;
    }

    /// <summary>Declare an Include column selection (mirrors the production <c>Consumes</c> mapping extension).</summary>
    public WallabyTestHarness Consumes<TEntity>(params string[] propertyNames)
        => SelectColumns<TEntity>(ColumnSelectionMode.Include, propertyNames);

    /// <summary>Declare an Exclude column selection (mirrors the production <c>ConsumesAllExcept</c> mapping extension).</summary>
    public WallabyTestHarness ConsumesAllExcept<TEntity>(params string[] propertyNames)
        => SelectColumns<TEntity>(ColumnSelectionMode.Exclude, propertyNames);

    private WallabyTestHarness SelectColumns<TEntity>(ColumnSelectionMode mode, string[] propertyNames)
    {
        if (!_columnSelections.TryGetValue(typeof(TEntity), out var list))
        {
            list = [];
            _columnSelections[typeof(TEntity)] = list;
        }
        list.Add(new ColumnSelection(mode, propertyNames));
        return this;
    }

    /// <summary>
    /// Supply the enrichment sessions transforms lease their queries from. Provider extensions call this
    /// (e.g. the EF Core <c>ForTestModel</c> factory registers a context-backed provider and
    /// <c>UseScopedContext</c> overrides it with a scope-key-aware one).
    /// </summary>
    public WallabyTestHarness UseEnrichmentSessions(IEnrichmentSessionProvider sessionProvider)
    {
        _sessionProvider = sessionProvider;
        return this;
    }

    /// <summary>
    /// Register a mapping built by a provider extension (which owns the transform invoker), mirroring the
    /// production <c>WithMappings(sink => sink.Map&lt;TEntity&gt;()...)</c> registration. The same entity may
    /// be mapped repeatedly (to different sinks); backfill versions are per type, last-wins.
    /// </summary>
    internal WallabyTestHarness AddMapping(EntityMapping mapping, bool backfill, string? backfillVersion)
    {
        _mappings.Add(mapping);
        if (backfill)
        {
            _backfillTypes[mapping.EntityClrType] = backfillVersion;
        }
        return this;
    }

    /// <summary>
    /// Declare a dependency: changes to the table behind <paramref name="navigation"/> should fan out
    /// and re-emit <typeparamref name="TPrimary"/>. Mirrors the production EF Core
    /// <c>DependsOn(...)</c> mapping extension; the expression is resolved by the storage provider
    /// when the capture plan is built.
    /// </summary>
    public WallabyTestHarness DependsOn<TPrimary, TNav>(Expression<Func<TPrimary, TNav>> navigation)
    {
        if (!_declaredDependencies.TryGetValue(typeof(TPrimary), out var list))
        {
            list = [];
            _declaredDependencies[typeof(TPrimary)] = list;
        }
        list.Add(navigation);
        return this;
    }

    // ---- lifecycle ----

    /// <summary>Validate the server and create the publication/slot/state schema for the whole model.</summary>
    public async Task SelfConfigureAsync(CancellationToken ct = default)
    {
        EnsureModel();
        await new PostgresSelfConfigurator(
                _dataSource,
                new SelfConfigOptions { SlotName = Names.Slot, PublicationName = Names.Publication },
                NullLogger.Instance,
                Instrumentation)
            .EnsureConfiguredAsync(_model!, ct);
    }

    /// <summary>Start the live pipeline (and backfill coordinator) in the background.</summary>
    public async Task StartAsync(CancellationToken ct = default)
    {
        EnsureModel();
        if (_pipelineTask is not null)
        {
            throw new InvalidOperationException("The harness is already running.");
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _spill = new PostgresUnloggedTableSpill(_dataSource, Names.Slot, Instrumentation);
        _stream = new LogicalReplicationStream(
            ConnectionString, Names.Slot, Names.Publication, _spill, model: _model, instrumentation: Instrumentation);
        _coordinator = new WatermarkBackfillCoordinator(
            _dataSource, new PostgresBackfillStore(_dataSource), NullLogger.Instance, Instrumentation)
        {
            ChunkSize = ChunkSize,
            Fence = VisibilityFence,
        };

        IChangeRouter router;
        if (_broadcast)
        {
            router = new BroadcastChangeRouter(_sinks.Keys.ToList());
        }
        else
        {
            // Sessions are late-bound here so UseEnrichmentSessions/UseScopedContext compose in any order
            // with the Map/Project calls that created the mappings.
            var sessions = _sessionProvider ?? throw new InvalidOperationException(
                "No enrichment session provider is registered. Create the harness through a provider " +
                "factory (e.g. WallabyTestHarness.ForTestModel) or call UseEnrichmentSessions(...).");
            router = new MappingChangeRouter(
                _mappings.Select(m => m with { Sessions = sessions }).ToList(),
                Instrumentation);
        }

        _dependentResolver = _model!.DependentBindings.Count > 0
            ? new DependentChangeResolver(
                _dataSource, _model, Instrumentation, MaxFanoutKeysPerTransaction, FanoutChunkSize)
            : null;
        _fanoutQueue = _dependentResolver is not null ? new PostgresFanoutQueueStore(_dataSource) : null;

        var reselector = ReselectUnavailableValues ? new RowReselector(_dataSource, _model!) : null;
        _pipeline = new WallabyPipeline(
            _stream, new ChangeEventFactory(_materializer!, reselector, NullLogger.Instance, Instrumentation),
            router, new SinkDispatcher(_sinks, NullLogger.Instance, Instrumentation, SinkRetry),
            new PostgresCheckpointStore(_dataSource), Names.Slot, NullLogger.Instance,
            MaxBatchSize, KeepaliveInterval, _coordinator, _dependentResolver, _fanoutQueue, Instrumentation,
            maxTransactionsPerBatch: MaxTransactionsPerBatch,
            backfillStore: new PostgresBackfillStore(_dataSource));

        // Mirror the production lifecycle: run one-time sink setup before streaming begins.
        foreach (var sink in _sinks.Values)
        {
            if (sink is ISinkInitializer initializer)
            {
                await initializer.InitializeAsync(_cts.Token);
            }
        }

        _pipelineTask = Task.Run(() => _pipeline.RunAsync(_cts.Token));

        if (HeartbeatInterval > TimeSpan.Zero)
        {
            var emitter = new HeartbeatEmitter(
                _dataSource, () => _pipeline.LastAcknowledgedLsn, HeartbeatInterval, NullLogger.Instance);
            _heartbeat = emitter;
            var token = _cts.Token;
            _heartbeatTask = Task.Run(async () =>
            {
                try { await emitter.RunAsync(token); }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { }
            });
        }
    }

    /// <summary>
    /// Drain the offloaded scoped fan-out queue once (running each due re-snapshot to completion). Call
    /// while the pipeline is running, since each scoped backfill round-trips watermarks through it.
    /// </summary>
    public async Task<int> DrainFanoutAsync()
    {
        if (_fanoutQueue is null || _coordinator is null || _cts is null)
        {
            return 0;
        }
        // The interval is unused here; DrainFanoutAsync only invokes DrainOnceAsync, not the polling loop.
        var worker = new FanoutQueueWorker(_fanoutQueue, _coordinator, _model!, NullLogger.Instance, TimeSpan.FromSeconds(1));
        return await worker.DrainOnceAsync(_cts.Token);
    }

    /// <summary>
    /// Run a backfill scheduler pass for the declared backfill tables (awaits completion). Returns the
    /// soonest time a backed-off (failing) table becomes due again, or null when none is pending a retry.
    /// </summary>
    /// <param name="version">Overrides the declared transform version for all backfill tables (e.g. to force a re-backfill).</param>
    public async Task<DateTimeOffset?> RunBackfillAsync(string? version = null)
        => await BuildScheduler(version).RunDueBackfillsAsync(_cts?.Token
            ?? throw new InvalidOperationException("Call StartAsync() before running a backfill."));

    /// <summary>
    /// Start the leader's backfill loop in the background: an initial due pass, then serving manual
    /// requests as they arrive (until <see cref="StopAsync"/>). Returns the loop task.
    /// </summary>
    /// <param name="pollInterval">Fallback poll interval; a request normally wakes the loop via NOTIFY.</param>
    /// <param name="version">Overrides the declared transform version for all backfill tables.</param>
    public Task RunBackfillLoopAsync(TimeSpan pollInterval, string? version = null)
    {
        if (_cts is null)
        {
            throw new InvalidOperationException("Call StartAsync() before running the backfill loop.");
        }
        if (_backfillLoopTask is not null)
        {
            throw new InvalidOperationException("The backfill loop is already running.");
        }

        var scheduler = BuildScheduler(version);
        var token = _cts.Token;
        _backfillLoopTask = Task.Run(async () =>
        {
            try { await scheduler.RunAsync(pollInterval, token); }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        });
        return _backfillLoopTask;
    }

    private BackfillScheduler BuildScheduler(string? version)
    {
        if (_coordinator is null)
        {
            throw new InvalidOperationException("Call StartAsync() before running a backfill.");
        }

        var tables = _backfillTypes
            .Select(kv => new BackfillTable(
                _model!.FindByClrType(kv.Key)!, version ?? kv.Value, PurgeOnVersionChange: false,
                PurgeTargetsFor(kv.Key)))
            .ToList();
        return new BackfillScheduler(
            tables, new PostgresBackfillStore(_dataSource), _coordinator,
            new SinkPurgeRunner(_sinks, Instrumentation, NullLogger.Instance),
            new BackfillSchedulerOptions(), NullLogger.Instance);
    }

    // Broadcast routes every change to every sink under its default destination; mapping mode mirrors
    // WallabyComponents.Build's per-mapping targets.
    private List<SinkPurgeTarget> PurgeTargetsFor(Type entityClrType)
        => _broadcast
            ? _sinks.Keys.Select(name => new SinkPurgeTarget(name, Destination: null, Scoped: false)).ToList()
            : _mappings
                .Where(m => m.EntityClrType == entityClrType)
                .Select(m => new SinkPurgeTarget(m.SinkName, m.Destination, m.DestinationSelector is not null))
                .Distinct()
                .ToList();

    /// <summary>Poll until the condition holds (or times out), surfacing any pipeline fault promptly.</summary>
    public Task WaitUntilAsync(Func<Task<bool>> predicate, TimeSpan? timeout = null)
        => Polling.UntilAsync(predicate, timeout, onTick: ThrowIfPipelineFaulted);

    public Task WaitUntilAsync(Func<bool> predicate, TimeSpan? timeout = null)
        => WaitUntilAsync(() => Task.FromResult(predicate()), timeout);

    /// <summary>Start, wait for the condition, then stop. The common one-shot case.</summary>
    public async Task RunUntilAsync(Func<Task<bool>> predicate, TimeSpan? timeout = null)
    {
        await StartAsync();
        try
        {
            await WaitUntilAsync(predicate, timeout);
        }
        finally
        {
            await StopAsync();
        }
    }

    public Task RunUntilAsync(Func<bool> predicate, TimeSpan? timeout = null)
        => RunUntilAsync(() => Task.FromResult(predicate()), timeout);

    /// <summary>Stop the pipeline, dispose the stream, and surface any fault.</summary>
    public async Task StopAsync()
    {
        if (_cts is not null)
        {
            await _cts.CancelAsync();
        }

        Exception? fault = null;
        if (_pipelineTask is not null)
        {
            try { await _pipelineTask; }
            catch (OperationCanceledException) { }
            catch (Exception ex) { fault = ex; }
        }

        if (_backfillLoopTask is not null)
        {
            try { await _backfillLoopTask; }
            catch (OperationCanceledException) { }
            catch (Exception ex) { fault ??= ex; }
        }

        if (_heartbeatTask is not null)
        {
            try { await _heartbeatTask; }
            catch (OperationCanceledException) { }
        }

        if (_stream is not null)
        {
            await _stream.DisposeAsync();
        }

        if (_spill is not null)
        {
            await _spill.DisposeAsync();
        }

        _cts?.Dispose();
        _cts = null;
        _pipelineTask = null;
        _backfillLoopTask = null;
        _heartbeatTask = null;
        _heartbeat = null;
        _stream = null;
        _spill = null;
        _pipeline = null;
        _coordinator = null;
        _dependentResolver = null;
        _fanoutQueue = null;

        if (fault is not null)
        {
            throw new InvalidOperationException("CDC pipeline faulted: " + fault.Message, fault);
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_pipelineTask is not null)
            {
                await StopAsync(); // throws when the pipeline faulted; cleanup below must still run
            }
        }
        finally
        {
            await SinkDisposal.DisposeAllAsync(_sinks.Values, NullLogger.Instance);
            // The shared session container caps max_replication_slots; leaving this test's slot behind
            // eventually starves a later test's slot creation.
            await PostgresReplicationCleanup.DropAsync(ConnectionString, Names);
            await _dataSource.DisposeAsync();
        }
    }

    private void ThrowIfPipelineFaulted()
    {
        if (_pipelineTask is { IsFaulted: true } task)
        {
            throw new InvalidOperationException("CDC pipeline faulted.", task.Exception?.GetBaseException());
        }
        if (_backfillLoopTask is { IsFaulted: true } loop)
        {
            throw new InvalidOperationException("Backfill loop faulted.", loop.Exception?.GetBaseException());
        }
    }

    private void EnsureModel()
    {
        if (_model is not null)
        {
            return;
        }

        var declared = _declaredDependencies.ToDictionary(
            kv => kv.Key,
            kv => (IReadOnlyList<LambdaExpression>)kv.Value);
        var plan = _modelProvider.BuildCapturePlan(new CaptureSpec
        {
            DeclaredEntities = [.. _capturedEntities.Union(_mappings.Select(m => m.EntityClrType))],
            DeclaredDependencies = declared,
            DeclaredColumnSelections = _columnSelections.ToDictionary(
                kv => kv.Key, kv => (IReadOnlyList<ColumnSelection>)kv.Value),
            // Mirrors WallabyConfiguration.ToCaptureSpec: scoped destinations and custom document ids
            // must be computable on deletes, which needs full old-row values.
            RequiresFullReplicaIdentity = _mappings
                .Where(m => m.DestinationSelector is not null || m.DocumentIdSelector is not null)
                .Select(m => m.EntityClrType)
                .ToHashSet(),
            // Harness scope keys are ChangeEvent-based, so only custom document ids are entity-bound.
            RequiresMaterializedEntity = _mappings
                .Where(m => m.DocumentIdSelector is not null)
                .Select(m => m.EntityClrType)
                .ToHashSet(),
        });
        _model = plan.Model;
        _materializer = plan.Materializer;
    }
}
