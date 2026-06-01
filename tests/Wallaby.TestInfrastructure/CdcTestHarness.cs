using System.Linq.Expressions;
using EFCore.CDC.TestModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Materialization;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace EFCore.CDC.TestInfrastructure;

/// <summary>
/// Drives a CDC pipeline against a real Postgres for integration tests, removing the per-test boilerplate
/// of self-config, pipeline wiring, backfill, lifecycle, and fault propagation.
/// </summary>
/// <remarks>
/// Typical one-shot use:
/// <code>
/// await using var harness = CdcTestHarness.ForTestModel(conn)
///     .AddSink(sink)
///     .Project&lt;Product&gt;("sink", index, p => new Dictionary&lt;string, object?&gt; { ["name"] = p.Name });
/// await harness.SelfConfigureAsync();
/// await harness.RunUntilAsync(() => /* effect observed */);
/// </code>
/// For backfill or multi-phase tests, use <see cref="StartAsync"/> + <see cref="RunBackfillAsync"/> +
/// <see cref="WaitUntilAsync(Func{Task{bool}}, TimeSpan?)"/> and let <c>await using</c> dispose/stop.
/// </remarks>
public sealed class CdcTestHarness : IAsyncDisposable
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
    private CdcModel? _cdcModel;
    private EntityMaterializer? _materializer;

    private CancellationTokenSource? _cts;
    private LogicalReplicationStream? _stream;
    private CdcPipeline? _pipeline;
    private WatermarkBackfillCoordinator? _coordinator;
    private DependentChangeResolver? _dependentResolver;
    private Task? _pipelineTask;

    public CdcTestHarness(string connectionString, Func<DbContext> contextFactory, CdcNames? names = null)
    {
        _connectionString = connectionString;
        _dataSource = NpgsqlDataSource.Create(connectionString);
        _newContext = contextFactory;
        Names = names ?? CdcNames.Unique();
        Db = new TestDatabase(connectionString);
    }

    /// <summary>Create a harness wired to the shared <see cref="AppDbContext"/> test model.</summary>
    public static CdcTestHarness ForTestModel(string connectionString, CdcNames? names = null)
        => new(connectionString, () => new AppDbContext(TestModelFactory.CreateOptions(connectionString)), names);

    public CdcNames Names { get; }
    public TestDatabase Db { get; }

    /// <summary>Telemetry holder threaded through the pipeline; attach a <c>MetricCollector</c>/<c>ActivityListener</c> for assertions.</summary>
    public WallabyInstrumentation Instrumentation { get; } = new();

    /// <summary>Backfill keyset page size (set before <see cref="StartAsync"/>).</summary>
    public int ChunkSize { get; set; } = 50;

    /// <summary>The highest LSN acknowledged to the server by the running pipeline.</summary>
    public ulong LastAcknowledgedLsn => _pipeline?.LastAcknowledgedLsn ?? 0;

    /// <summary>A backfill manager bound to this harness's database (for manual <c>RequestBackfillAsync</c>).</summary>
    public ICdcBackfillManager BackfillManager
    {
        get { EnsureModel(); return new DefaultBackfillManager(_cdcModel!, new PostgresBackfillStore(_dataSource)); }
    }

    // ---- configuration ----

    public CdcTestHarness AddSink(ISink sink)
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
    public CdcTestHarness Broadcast()
    {
        _broadcast = true;
        return this;
    }

    /// <summary>Map an entity to a sink/destination via a full transform (with <see cref="DbContext"/> access).</summary>
    public CdcTestHarness Map<TEntity>(
        string sink,
        string? destination,
        Func<DbContext, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, CdcDocument?>>> transform,
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
    public CdcTestHarness Project<TEntity>(
        string sink, string? destination, Func<TEntity, CdcDocument?> document, bool backfill = false, string? backfillVersion = null,
        Func<TEntity, object?>? scopeKey = null, Func<object?, string?>? scopedDestination = null)
        where TEntity : class
        => Map<TEntity>(sink, destination, (_, changes, _) =>
        {
            var documents = new Dictionary<DocumentKey, CdcDocument?>();
            foreach (var change in changes)
            {
                documents[change.Key] = document(change.Entity!);
            }
            return Task.FromResult<IReadOnlyDictionary<DocumentKey, CdcDocument?>>(documents);
        }, backfill, backfillVersion, scopeKey, scopedDestination);

    /// <summary>Build the enrichment <see cref="DbContext"/> from a row's scope key (for tenant tests).</summary>
    public CdcTestHarness UseScopedContext(Func<object?, DbContext> factory)
    {
        _scopedContextFactory = factory;
        return this;
    }

    /// <summary>
    /// Declare a dependency: changes to the table behind <paramref name="navigation"/> should fan out
    /// and re-emit <typeparamref name="TPrimary"/>. Mirrors the production
    /// <c>EntityMapBuilder.DependsOn(...)</c> API.
    /// </summary>
    public CdcTestHarness DependsOn<TPrimary, TNav>(Expression<Func<TPrimary, TNav>> navigation)
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
    public Task StartAsync(CancellationToken ct = default)
    {
        EnsureModel();
        if (_pipelineTask is not null)
        {
            throw new InvalidOperationException("The harness is already running.");
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _stream = new LogicalReplicationStream(_connectionString, Names.Slot, Names.Publication);
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

        _pipeline = new CdcPipeline(
            _stream, new ChangeEventFactory(_materializer!), router, new SinkDispatcher(_sinks, instrumentation: Instrumentation),
            new PostgresCheckpointStore(_dataSource), Names.Slot, NullLogger.Instance, _coordinator, _dependentResolver, Instrumentation);

        _pipelineTask = Task.Run(() => _pipeline.RunAsync(_cts.Token));
        return Task.CompletedTask;
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

        _cts?.Dispose();
        _cts = null;
        _pipelineTask = null;
        _stream = null;
        _pipeline = null;
        _coordinator = null;
        _dependentResolver = null;

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

    private sealed class NullServiceProvider : IServiceProvider
    {
        public static readonly NullServiceProvider Instance = new();
        public object? GetService(Type serviceType) => null;
    }
}
