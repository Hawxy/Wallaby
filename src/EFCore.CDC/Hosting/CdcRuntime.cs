using System.Linq.Expressions;
using EFCore.CDC.Abstractions;
using EFCore.CDC.DependencyInjection;
using EFCore.CDC.Internal;
using EFCore.CDC.Internal.Backfill;
using EFCore.CDC.Internal.Materialization;
using EFCore.CDC.Internal.Pipeline;
using EFCore.CDC.Internal.Replication;
using EFCore.CDC.Internal.SelfConfig;
using EFCore.CDC.Internal.State;
using EFCore.CDC.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.Logging;

namespace EFCore.CDC.Hosting;

/// <summary>
/// Owns the end-to-end CDC lifecycle for a context: elect leadership (cluster lock), self-configure,
/// then run the live pipeline and backfill scheduler. Standby nodes wait and take over on failover.
/// </summary>
internal sealed class CdcRuntime<TContext> where TContext : DbContext
{
    private readonly IDbContextFactory<TContext> _dbContextFactory;
    private readonly CdcConfiguration _config;
    private readonly CdcOptions _options;
    private readonly CdcDataSource _dataSource;
    private readonly IClusterLock _clusterLock;
    private readonly IServiceProvider _services;
    private readonly ILogger<CdcRuntime<TContext>> _logger;

    // Built once.
    private CdcModel _cdcModel = null!;
    private EntityMaterializer _materializer = null!;
    private MappingChangeRouter _router = null!;
    private SinkDispatcher _dispatcher = null!;
    private WatermarkBackfillCoordinator _coordinator = null!;
    private PostgresSelfConfigurator _selfConfigurator = null!;
    private PostgresCheckpointStore _checkpoints = null!;
    private DependentChangeResolver? _dependentResolver;
    private IReadOnlyList<(CapturedTable Table, string? Version)> _backfillTables = [];

    public CdcRuntime(
        IDbContextFactory<TContext> dbContextFactory,
        CdcConfiguration config,
        CdcDataSource dataSource,
        IClusterLock clusterLock,
        IServiceProvider services,
        ILogger<CdcRuntime<TContext>> logger)
    {
        _dbContextFactory = dbContextFactory;
        _config = config;
        _options = config.Options;
        _dataSource = dataSource;
        _clusterLock = clusterLock;
        _services = services;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        BuildComponents();

        while (!ct.IsCancellationRequested)
        {
            IClusterLockHandle? leadership = null;
            try
            {
                leadership = await _clusterLock.TryAcquireAsync(_options.SlotName, ct);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to acquire CDC leadership; retrying.");
            }

            if (leadership is null)
            {
                _logger.LogDebug("CDC standby: another node holds leadership for slot '{Slot}'.", _options.SlotName);
                await DelaySafeAsync(_options.StandbyRetryInterval, ct);
                continue;
            }

            await using (leadership)
            {
                _logger.LogInformation("Acquired CDC leadership for slot '{Slot}'.", _options.SlotName);
                try
                {
                    await RunAsLeaderAsync(ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "CDC leader session failed; will retry.");
                    await DelaySafeAsync(_options.LeaderRetryInterval, ct);
                }
            }
        }
    }

    private async Task RunAsLeaderAsync(CancellationToken ct)
    {
        await _selfConfigurator.EnsureConfiguredAsync(_cdcModel, ct);

        await using var stream = new LogicalReplicationStream(_dataSource.ConnectionString, _options.SlotName, _options.PublicationName);
        var pipeline = new CdcPipeline(
            stream, new ChangeEventFactory(_materializer), _router, _dispatcher, _checkpoints, _options.SlotName, _logger,
            _coordinator, _dependentResolver);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var scheduler = new BackfillScheduler(
            _backfillTables, new PostgresBackfillStore(_dataSource.Source), _coordinator,
            new BackfillSchedulerOptions
            {
                AutoBackfillNewTables = _options.AutoBackfillNewTables,
                AutoBackfillOnVersionChange = _options.AutoBackfillOnVersionChange,
            },
            _logger);

        var backfillTask = Task.Run(async () =>
        {
            try { await scheduler.RunDueBackfillsAsync(linked.Token); }
            catch (OperationCanceledException) { }
            catch (Exception ex) { _logger.LogError(ex, "Backfill scheduler failed."); }
        }, linked.Token);

        try
        {
            await pipeline.RunAsync(linked.Token);
        }
        finally
        {
            await linked.CancelAsync();
            try { await backfillTask; } catch { /* already logged */ }
        }
    }

    private void BuildComponents()
    {
        var declaredDependencies = new Dictionary<Type, IReadOnlyList<LambdaExpression>>();
        foreach (var mapping in _config.Mappings.Values)
        {
            if (mapping.DeclaredDependencies.Count > 0)
            {
                declaredDependencies[mapping.EntityClrType] = mapping.DeclaredDependencies;
            }
        }

        var captureSpec = new CaptureSpec
        {
            CaptureAllMapped = _config.CaptureAllMapped,
            DeclaredEntities = _config.DeclaredEntities,
            RequiresFullReplicaIdentity = _config.RequiresFullReplicaIdentity,
            DeclaredDependencies = declaredDependencies,
        };

        IModel model;
        using (var context = _dbContextFactory.CreateDbContext())
        {
            model = context.Model;
        }

        _cdcModel = ModelToCdcModel.Build(model, captureSpec);
        _materializer = new EntityMaterializer(model);

        var mappings = new Dictionary<Type, EntityMapping>();
        var backfillTables = new List<(CapturedTable, string?)>();
        foreach (var registration in _config.Mappings.Values)
        {
            var captured = _cdcModel.FindByClrType(registration.EntityClrType)
                ?? throw new CdcConfigurationException(
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

        var sinks = _config.Sinks.ToDictionary(s => s.Name, s => s.Factory(_services));

        IEnrichmentContextProvider contextProvider = _config.ScopedContextFactory is { } scopedFactory
            ? new ScopedEnrichmentContextProvider(scopedFactory, _services)
            : new DefaultEnrichmentContextProvider(() => _dbContextFactory.CreateDbContext());
        _router = new MappingChangeRouter(mappings, contextProvider);
        _dispatcher = new SinkDispatcher(sinks, skipFailedBatches: _options.DeadLetterPolicy == CdcDeadLetterPolicy.Skip, _logger);
        _coordinator = new WatermarkBackfillCoordinator(
            _dataSource.Source, new PostgresBackfillStore(_dataSource.Source), _logger) { ChunkSize = _options.ChunkSize };
        _dependentResolver = _cdcModel.DependentBindings.Count > 0
            ? new DependentChangeResolver(_dataSource.Source, _cdcModel)
            : null;
        _checkpoints = new PostgresCheckpointStore(_dataSource.Source);
        _selfConfigurator = new PostgresSelfConfigurator(
            _dataSource.Source,
            new SelfConfigOptions
            {
                SlotName = _options.SlotName,
                PublicationName = _options.PublicationName,
                ManagePublicationTables = _options.ManagePublicationTables,
                RequireFullReplicaIdentity = _options.RequireFullReplicaIdentity,
            },
            _logger);
        _backfillTables = backfillTables;
    }

    private async Task DelaySafeAsync(TimeSpan delay, CancellationToken ct)
    {
        try { await Task.Delay(delay, ct); }
        catch (OperationCanceledException) { }
    }
}
