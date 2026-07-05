using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Materialization;
using Wallaby.Internal.Pipeline;
using Wallaby.Testing;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;
using Wallaby.Model;
using Wallaby.TestModel;

namespace Wallaby.TestInfrastructure;

/// <summary>
/// Drives a CDC pipeline against a real Postgres for integration tests, removing the per-test boilerplate
/// of self-config, pipeline wiring, backfill, lifecycle, and fault propagation.
/// </summary>
/// <remarks>
/// Typical one-shot use:
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
    private readonly string _connectionString;
    private readonly NpgsqlDataSource _dataSource;
    private readonly Func<DbContext> _newContext;
    private readonly Dictionary<string, ISink> _sinks = [];
    private readonly Dictionary<Type, EntityMapping> _mappings = [];
    private readonly Dictionary<Type, string?> _backfillTypes = [];
    private readonly Dictionary<Type, List<LambdaExpression>> _declaredDependencies = [];
    private bool _broadcast;
    private Func<object?, DbContext>? _scopedContextFactory;

    private IModel? _model;
    private WallabyModel? _cdcModel;
    private EntityMaterializer? _materializer;

    private CancellationTokenSource? _cts;
    private ITransactionSpill? _spill;
    private LogicalReplicationStream? _stream;
    private CdcPipeline? _pipeline;
    private WatermarkBackfillCoordinator? _coordinator;
    private DependentChangeResolver? _dependentResolver;
    private IFanoutQueueStore? _fanoutQueue;
    private Task? _pipelineTask;

    public WallabyTestHarness(string connectionString, Func<DbContext> contextFactory, WallabyNames? names = null)
    {
        _connectionString = connectionString;
        _dataSource = NpgsqlDataSource.Create(connectionString);
        _newContext = contextFactory;
        Names = names ?? WallabyNames.Unique();
        Db = new TestDatabase(connectionString);
    }

    /// <summary>Create a harness wired to the shared <see cref="AppDbContext"/> test model.</summary>
    public static WallabyTestHarness ForTestModel(string connectionString, WallabyNames? names = null)
        => new(connectionString, () => new AppDbContext(TestModelFactory.CreateOptions(connectionString)), names);

    public WallabyNames Names { get; }
    public TestDatabase Db { get; }

    /// <summary>Telemetry holder threaded through the pipeline; attach a <c>MetricCollector</c>/<c>ActivityListener</c> for assertions.</summary>
    public WallabyInstrumentation Instrumentation { get; } = new();

    /// <summary>Backfill keyset page size (set before <see cref="StartAsync"/>).</summary>
    public int ChunkSize { get; set; } = 50;

    /// <summary>Maximum records per dispatched batch and per inline fan-out page (set before <see cref="StartAsync"/>).</summary>
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>Interval for in-flight replication keepalives during transaction processing (set before <see cref="StartAsync"/>).</summary>
    public TimeSpan KeepaliveInterval { get; set; } = TimeSpan.FromSeconds(10);

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
        get { EnsureModel(); return new DefaultBackfillManager(_cdcModel!, new PostgresBackfillStore(_dataSource)); }
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

    /// <summary>Map an entity to a sink/destination via a full transform (with <see cref="DbContext"/> access).</summary>
    public WallabyTestHarness Map<TEntity>(
        string sink,
        string? destination,
        Func<DbContext, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> transform,
        bool backfill = false,
        string? backfillVersion = null,
        Func<TEntity, object?>? scopeKey = null,
        Func<object?, string?>? scopedDestination = null)
        where TEntity : class
    {
        _mappings[typeof(TEntity)] = new EntityMapping
        {
            EntityClrType = typeof(TEntity),
            SinkName = sink,
            Destination = destination,
            BackfillVersion = backfillVersion,
            Transform = new TransformInvoker<TEntity>(new DelegateTransform<TEntity>(transform)),
            ScopeKeySelector = scopeKey is null ? null : change => change.Entity is TEntity e ? scopeKey(e) : null,
            DestinationSelector = scopedDestination,
        };
        if (backfill)
        {
            _backfillTypes[typeof(TEntity)] = backfillVersion;
        }
        return this;
    }

    /// <summary>Map an entity to a sink/destination via a simple per-row projection.</summary>
    public WallabyTestHarness Project<TEntity>(
        string sink, string? destination, Func<TEntity, WallabyDocument?> document, bool backfill = false, string? backfillVersion = null,
        Func<TEntity, object?>? scopeKey = null, Func<object?, string?>? scopedDestination = null)
        where TEntity : class
        => Map<TEntity>(sink, destination, (_, changes, _) =>
        {
            var documents = new Dictionary<DocumentKey, WallabyDocument?>();
            foreach (var change in changes)
            {
                documents[change.Key] = document(change.Entity!);
            }
            return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
        }, backfill, backfillVersion, scopeKey, scopedDestination);

    /// <summary>Build the enrichment <see cref="DbContext"/> from a row's scope key (for tenant tests).</summary>
    public WallabyTestHarness UseScopedContext(Func<object?, DbContext> factory)
    {
        _scopedContextFactory = factory;
        return this;
    }

    /// <summary>
    /// Declare a dependency: changes to the table behind <paramref name="navigation"/> should fan out
    /// and re-emit <typeparamref name="TPrimary"/>. Mirrors the production
    /// <c>EntityMapBuilder.DependsOn(...)</c> API.
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
                NullLogger.Instance)
            .EnsureConfiguredAsync(_cdcModel!, ct);
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
        _spill = new PostgresUnloggedTableSpill(_dataSource, Names.Slot);
        _stream = new LogicalReplicationStream(_connectionString, Names.Slot, Names.Publication, _spill);
        _coordinator = new WatermarkBackfillCoordinator(
            _dataSource, new PostgresBackfillStore(_dataSource), NullLogger.Instance, Instrumentation) { ChunkSize = ChunkSize };

        IEnrichmentContextProvider contextProvider = _scopedContextFactory is { } scoped
            ? new ScopedEnrichmentContextProvider((key, _) => scoped(key), NullServiceProvider.Instance)
            : new DefaultEnrichmentContextProvider(_newContext);
        IChangeRouter router = _broadcast
            ? new BroadcastChangeRouter(_sinks.Keys.ToList())
            : new MappingChangeRouter(_mappings, contextProvider, Instrumentation);

        _dependentResolver = _cdcModel!.DependentBindings.Count > 0
            ? new DependentChangeResolver(_dataSource, _cdcModel, Instrumentation)
            : null;
        _fanoutQueue = _dependentResolver is not null ? new PostgresFanoutQueueStore(_dataSource) : null;

        _pipeline = new CdcPipeline(
            _stream, new ChangeEventFactory(_materializer!), router, new SinkDispatcher(_sinks, Instrumentation, SinkRetry),
            new PostgresCheckpointStore(_dataSource), Names.Slot, NullLogger.Instance,
            MaxBatchSize, KeepaliveInterval, _coordinator, _dependentResolver, _fanoutQueue, Instrumentation);

        // Mirror the production lifecycle: run one-time sink setup before streaming begins.
        foreach (var sink in _sinks.Values)
        {
            if (sink is ISinkInitializer initializer)
            {
                await initializer.InitializeAsync(_cts.Token);
            }
        }

        _pipelineTask = Task.Run(() => _pipeline.RunAsync(_cts.Token));
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
        // The interval is unused here — DrainFanoutAsync only invokes DrainOnceAsync, not the polling loop.
        var worker = new FanoutQueueWorker(_fanoutQueue, _coordinator, _cdcModel!, NullLogger.Instance, TimeSpan.FromSeconds(1));
        return await worker.DrainOnceAsync(_cts.Token);
    }

    /// <summary>Run a backfill scheduler pass for the declared backfill tables (awaits completion).</summary>
    /// <param name="version">Overrides the declared transform version for all backfill tables (e.g. to force a re-backfill).</param>
    public async Task RunBackfillAsync(string? version = null)
    {
        if (_coordinator is null || _cts is null)
        {
            throw new InvalidOperationException("Call StartAsync() before running a backfill.");
        }

        var tables = _backfillTypes
            .Select(kv => (_cdcModel!.FindByClrType(kv.Key)!, version ?? kv.Value))
            .ToList();

        var scheduler = new BackfillScheduler(
            tables, new PostgresBackfillStore(_dataSource), _coordinator, new BackfillSchedulerOptions(), NullLogger.Instance);
        await scheduler.RunDueBackfillsAsync(_cts.Token);
    }

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
        if (_pipelineTask is not null)
        {
            await StopAsync();
        }
        await _dataSource.DisposeAsync();
    }

    private void ThrowIfPipelineFaulted()
    {
        if (_pipelineTask is { IsFaulted: true } task)
        {
            throw new InvalidOperationException("CDC pipeline faulted.", task.Exception?.GetBaseException());
        }
    }

    private void EnsureModel()
    {
        if (_cdcModel is not null)
        {
            return;
        }

        using var context = _newContext();
        _model = context.Model;
        var declared = _declaredDependencies.ToDictionary(
            kv => kv.Key,
            kv => (IReadOnlyList<LambdaExpression>)kv.Value);
        _cdcModel = ModelToCdcModel.Build(_model, new CaptureSpec
        {
            CaptureAllMapped = true,
            DeclaredDependencies = declared,
        });
        _materializer = new EntityMaterializer(_model);
    }
    
    private sealed class NullServiceProvider : IServiceProvider, IServiceScopeFactory, IServiceScope
    {
        public static readonly NullServiceProvider Instance = new();
        public object? GetService(Type serviceType) => serviceType == typeof(IServiceScopeFactory) ? this : null;
        public IServiceScope CreateScope() => this;
        public IServiceProvider ServiceProvider => this;
        public void Dispose() { }
    }
}
