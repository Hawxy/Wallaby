using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Internal;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Hosting;

/// <summary>
/// The runtime's long-lived components, wired once from the resolved configuration and shared by every
/// leader session: the merged capture model and its materializer, the routing/dispatch pair, the backfill
/// coordinator and state stores, and the self-configurator. Per-term resources (the spill, replication
/// stream, and pipeline) are created by <see cref="LeaderSession"/>.
/// </summary>
internal sealed class WallabyComponents
{
    public required WallabyModel Model { get; init; }
    public required IRowMaterializer Materializer { get; init; }
    public required MappingChangeRouter Router { get; init; }
    public required SinkDispatcher Dispatcher { get; init; }
    public required IReadOnlyDictionary<string, ISink> Sinks { get; init; }
    public required WatermarkBackfillCoordinator Coordinator { get; init; }
    public required ISelfConfigurator SelfConfigurator { get; init; }

    /// <summary>The checkpoint store the pipeline writes through (throttled when configured).</summary>
    public required ICheckpointStore Checkpoints { get; init; }

    /// <summary>The unthrottled store, for reads and writes that must be durable immediately (slot-gap repair).</summary>
    public required PostgresCheckpointStore CheckpointsDirect { get; init; }

    public required IBackfillStateStore BackfillStore { get; init; }
    public DependentChangeResolver? DependentResolver { get; init; }
    public IFanoutQueueStore? FanoutQueue { get; init; }

    /// <summary>Every mapped table with the composite of its mappings' declared backfill versions.</summary>
    public required IReadOnlyList<(CapturedTable Table, string? Version)> BackfillTables { get; init; }

    public static WallabyComponents Build(
        ResolvedProviderSet providers,
        WallabyConfiguration config,
        WallabyOptions options,
        WallabyDataSource dataSource,
        IServiceProvider services,
        WallabyInstrumentation instrumentation,
        WallabyStatus status,
        ILogger logger)
    {
        var model = providers.MergedPlan.Model;

        var mappings = new List<EntityMapping>();
        // Backfill state is per table: a table mapped to several sinks appears once, with the composite
        // of its mappings' declared versions as the version key.
        var backfillByTable = new Dictionary<string, (CapturedTable Table, List<string> Versions)>(StringComparer.Ordinal);
        foreach (var sink in config.Sinks)
        {
            foreach (var registration in sink.Mappings)
            {
                var captured = model.FindByClrType(registration.EntityClrType)
                    ?? throw new WallabyConfigurationException(
                        $"Mapped entity '{registration.EntityClrType.FullName}' is not captured. Ensure it is declared and mapped to a table.");

                mappings.Add(new EntityMapping
                {
                    EntityClrType = registration.EntityClrType,
                    SinkName = sink.Name,
                    Destination = registration.Destination,
                    Transform = registration.TransformFactory!(services),
                    Sessions = providers.ProviderByMappedType[registration.EntityClrType].Sessions,
                    DocumentIdSelector = registration.DocumentIdSelector,
                    ScopeKeySelector = registration.ScopeKeySelector,
                    DestinationSelector = registration.DestinationSelector,
                });

                if (!backfillByTable.TryGetValue(captured.QualifiedName, out var entry))
                {
                    backfillByTable[captured.QualifiedName] = entry = (captured, []);
                }
                if (registration.BackfillVersion is not null)
                {
                    entry.Versions.Add(registration.BackfillVersion);
                }
            }
        }
        var backfillTables = backfillByTable.Values
            .Select(v => (v.Table, BackfillVersioning.Compose(v.Versions)))
            .ToList();

        var sinks = config.Sinks.ToDictionary(s => s.Name, s => s.Factory(services));
        var backfillStore = new PostgresBackfillStore(dataSource.Source);
        var checkpointsDirect = new PostgresCheckpointStore(dataSource.Source);
        var dependentResolver = model.DependentBindings.Count > 0
            ? new DependentChangeResolver(dataSource.Source, model, instrumentation)
            : null;

        return new WallabyComponents
        {
            Model = model,
            Materializer = providers.MergedPlan.Materializer,
            Router = new MappingChangeRouter(mappings, instrumentation),
            Dispatcher = new SinkDispatcher(sinks, instrumentation, options.SinkRetry, status),
            Sinks = sinks,
            Coordinator = new WatermarkBackfillCoordinator(
                dataSource.Source, backfillStore, logger, instrumentation) { ChunkSize = options.ChunkSize },
            SelfConfigurator = new PostgresSelfConfigurator(
                dataSource.Source,
                new SelfConfigOptions
                {
                    SlotName = options.SlotName,
                    PublicationName = options.PublicationName,
                    ManagePublicationTables = options.ManagePublicationTables,
                    RequireFullReplicaIdentity = options.RequireFullReplicaIdentity,
                    ExternalSlots = ExternalSlotResolver.Resolve(config.ExternalSlots, providers.ModelProviders),
                },
                logger),
            Checkpoints = options.Advanced.CheckpointSaveInterval > TimeSpan.Zero
                ? new ThrottledCheckpointStore(checkpointsDirect, options.Advanced.CheckpointSaveInterval)
                : checkpointsDirect,
            CheckpointsDirect = checkpointsDirect,
            BackfillStore = backfillStore,
            DependentResolver = dependentResolver,
            FanoutQueue = dependentResolver is not null ? new PostgresFanoutQueueStore(dataSource.Source) : null,
            BackfillTables = backfillTables,
        };
    }
}
