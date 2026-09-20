using System.Linq.Expressions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Internal;
using Wallaby.Providers;

namespace Wallaby.DependencyInjection;

/// <summary>A registered sink: its name, a factory that resolves the instance, and its attached mappings.</summary>
internal sealed class SinkRegistration
{
    public required string Name { get; init; }

    /// <summary>Resolves the sink instance. Settable so Wallaby.Testing can swap in a test double in place.</summary>
    public required Func<IServiceProvider, ISink> Factory { get; set; }

    /// <summary>The entity mappings attached via <c>WithMappings(...)</c>. An empty list is a valid sink.</summary>
    public List<MappingRegistration> Mappings { get; } = [];
}

/// <summary>An entity mapping attached to a sink, plus the factory for its transform invoker.</summary>
internal sealed class MappingRegistration
{
    public required Type EntityClrType { get; init; }
    public string? Destination { get; set; }
    public string? BackfillVersion { get; set; }

    /// <summary>Purge this mapping's sink destinations before the fresh backfill a version change triggers.</summary>
    public bool PurgeOnBackfillVersionChange { get; set; }
    public Func<IServiceProvider, IWallabyTransformInvoker>? TransformFactory { get; set; }
    public Func<ChangeEvent, string>? DocumentIdSelector { get; set; }

    /// <summary>Provider name pinned by <c>FromProvider(...)</c>; wins over <see cref="TransformProviderName"/>.</summary>
    public string? ExplicitProviderName { get; set; }

    /// <summary>Provider name implied by a provider-typed <c>UsingTransform</c> (its session type fixes the provider).</summary>
    public string? TransformProviderName { get; set; }

    /// <summary>
    /// The provider this mapping is pinned to, or null to auto-resolve by probing each provider's model.
    /// An explicit <c>FromProvider(...)</c> wins over the transform-implied name; a conflict between the
    /// two fails at <see cref="WallabyBuilder"/> build time.
    /// </summary>
    public string? ProviderName => ExplicitProviderName ?? TransformProviderName;

    /// <summary>The properties this mapping's transform consumes; null = all mapped properties.</summary>
    public ColumnSelection? ColumnSelection { get; set; }

    /// <summary>Per-row scope key (e.g. tenant id) for enrichment-session + destination scoping.</summary>
    public Func<ChangeEvent, object?>? ScopeKeySelector { get; set; }

    /// <summary>Per-scope-key destination (e.g. index-per-tenant); falls back to <see cref="Destination"/>.</summary>
    public Func<object?, string?>? DestinationSelector { get; set; }

    /// <summary>Set by <c>KeyedBy</c>: the document id is computed from the entity, deletes included.</summary>
    public bool HasEntityKeyedId { get; set; }

    /// <summary>Set by the entity-typed <c>ScopedBy</c> overload (the <see cref="ChangeEvent"/> overload reads captured columns instead).</summary>
    public bool HasEntityScopedKey { get; set; }

    /// <summary>
    /// Set by a provider's tenant-scoping extension (Marten's <c>ScopedByTenant()</c>): the scope key is
    /// the row's tenant id, so the provider's model must carry a tenant column for the entity.
    /// </summary>
    public bool ScopedByTenantId { get; set; }

    /// <summary>
    /// Delete-time identity or routing is computed from the materialized entity, so a delete without one
    /// targets the wrong document/destination. Escalates the table's replica-identity check to an error.
    /// </summary>
    public bool RequiresMaterializedEntity => HasEntityKeyedId || (HasEntityScopedKey && DestinationSelector is not null);

    /// <summary>
    /// Navigation expressions declared via <c>DependsOn(...)</c>. Each is a single one-hop navigation whose
    /// target/join table is captured and fans changes out to this entity. Resolved against the storage
    /// provider's model at startup (via <see cref="IWallabyModelProvider.BuildCapturePlan"/>).
    /// </summary>
    public List<LambdaExpression> DeclaredDependencies { get; } = [];
}

/// <summary>
/// A declared external replication slot: an additional pgoutput publication + slot that Wallaby
/// provisions (and reconciles) for a third-party CDC consumer (e.g. an ELT tool) but never consumes.
/// Table declarations are resolved to schema-qualified names against the storage provider's model at startup.
/// </summary>
internal sealed class ExternalSlotRegistration
{
    public required string SlotName { get; init; }

    /// <summary>Optional publication name; defaults to <c>"{SlotName}_pub"</c> when unset.</summary>
    public string? PublicationName { get; set; }

    /// <summary>The effective publication name: <see cref="PublicationName"/>, or <c>"{SlotName}_pub"</c> when unset (matching <c>ExternalSlotResolver</c>).</summary>
    public string ResolvedPublicationName => string.IsNullOrWhiteSpace(PublicationName) ? $"{SlotName}_pub" : PublicationName;

    /// <summary>Tables declared by schema-qualified name.</summary>
    public List<(string Schema, string Table)> TableNames { get; } = [];

    /// <summary>Tables declared by entity CLR type, resolved against the storage provider's model at startup.</summary>
    public List<Type> EntityTypes { get; } = [];

    /// <summary>True when the slot includes every table the registered providers model.</summary>
    public bool AllEntities { get; set; }

    /// <summary>Tables excluded by schema-qualified name from the <see cref="AllEntities"/> set.</summary>
    public List<(string Schema, string Table)> ExcludedTableNames { get; } = [];

    /// <summary>Tables excluded by entity CLR type from the <see cref="AllEntities"/> set.</summary>
    public List<Type> ExcludedEntityTypes { get; } = [];

    /// <summary>True when resolving the slot's tables needs the storage providers' models.</summary>
    public bool NeedsModel => AllEntities || EntityTypes.Count > 0 || ExcludedEntityTypes.Count > 0;

    /// <summary>True when any exclusion was declared.</summary>
    public bool HasExclusions => ExcludedTableNames.Count > 0 || ExcludedEntityTypes.Count > 0;
}

/// <summary>The result of the fluent builder, consumed by the runtime.</summary>
internal sealed class WallabyConfiguration
{
    /// <summary>
    /// Option mutations queued by <see cref="WallabyBuilder.ConfigureOptions(Action{WallabyOptions})"/> and
    /// <see cref="WallabyBuilder.UseConnectionString(string)"/> (and their provider-aware overloads).
    /// Applied by the options pipeline at the <c>AddWallaby</c> registration position, so they compose with
    /// <c>Configure&lt;WallabyOptions&gt;</c>/<c>PostConfigure</c> calls in registration order. The provider
    /// passed in is the root provider.
    /// </summary>
    public List<Action<IServiceProvider, WallabyOptions>> OptionsActions { get; } = [];

    public List<SinkRegistration> Sinks { get; } = [];

    /// <summary>Every mapping across all sinks, in sink-then-declaration order.</summary>
    public IEnumerable<MappingRegistration> AllMappings => Sinks.SelectMany(s => s.Mappings);

    /// <summary>External pgoutput publication+slot pairs to provision for third-party consumers (e.g. ELT).</summary>
    public List<ExternalSlotRegistration> ExternalSlots { get; } = [];

    /// <summary>
    /// Builds the <see cref="ITransactionSpill"/> that buffers a pgoutput v2 streamed (large) transaction until
    /// commit. Set by <c>SpillToDisk</c>/<c>SpillToDatabase</c>/<c>UseTransactionSpill</c>; null selects the
    /// default database-backed spill. Invoked once per leader session with the runtime's <see cref="SpillContext"/>.
    /// </summary>
    public Func<SpillContext, ITransactionSpill>? SpillFactory { get; set; }

    /// <summary>
    /// Supplies the password (typically a short-lived cloud token) for every connection Wallaby opens.
    /// Set by <see cref="WallabyBuilder.UsePasswordProvider(Func{CancellationToken, ValueTask{string}}, TimeSpan?)"/>;
    /// null uses the password in the connection string.
    /// </summary>
    public Func<IServiceProvider, CancellationToken, ValueTask<string>>? PasswordProvider { get; set; }

    /// <summary>How long the pool caches a provided password before asking <see cref="PasswordProvider"/> again.</summary>
    public TimeSpan PasswordRefreshInterval { get; set; } = WallabyDataSource.DefaultPasswordRefreshInterval;

    /// <summary>Extra data-source configuration, applied after Wallaby's own settings.</summary>
    public Action<NpgsqlDataSourceBuilder>? ConfigureDataSource { get; set; }

    /// <summary>
    /// The registered storage providers, in registration order. Empty when no provider is registered
    /// (provision-only). Each provider derives its own capture plan; the plans are merged into one model
    /// sharing a single slot/publication/checkpoint. Names are unique (enforced by
    /// <see cref="WallabyBuilder.UseProvider"/>).
    /// </summary>
    public List<WallabyProviderRegistration> Providers { get; } = [];

    /// <summary>
    /// True when a sink is declared (mappings only exist attached to one). When false, Wallaby runs
    /// provision-only: it creates the declared external slots and never opens a primary slot or streams.
    /// </summary>
    public bool CaptureIntended => Sinks.Count > 0;

    /// <summary>
    /// Build the <see cref="CaptureSpec"/> for one provider: the declared entities, replica-identity flags,
    /// and <c>DependsOn(...)</c> navigations whose mapping resolved to <paramref name="providerName"/> (per
    /// <paramref name="affinities"/>).
    /// </summary>
    public CaptureSpec ToCaptureSpec(string providerName, IReadOnlyDictionary<Type, string> affinities)
    {
        // A type mapped to several sinks appears once: entities dedupe, dependencies merge.
        var declaredEntities = new List<Type>();
        var requiresFullReplicaIdentity = new HashSet<Type>();
        var requiresMaterializedEntity = new HashSet<Type>();
        var requiresTenantColumn = new HashSet<Type>();
        var declaredDependencies = new Dictionary<Type, List<LambdaExpression>>();
        var columnSelections = new Dictionary<Type, List<ColumnSelection>>();
        var consumesAll = new HashSet<Type>();
        foreach (var mapping in AllMappings)
        {
            if (affinities[mapping.EntityClrType] != providerName)
            {
                continue;
            }

            if (!declaredEntities.Contains(mapping.EntityClrType))
            {
                declaredEntities.Add(mapping.EntityClrType);
            }
            // Scoped destinations and custom document ids must both be computable on deletes,
            // which needs full old-row values.
            if (mapping.DestinationSelector is not null || mapping.DocumentIdSelector is not null)
            {
                requiresFullReplicaIdentity.Add(mapping.EntityClrType);
            }
            if (mapping.RequiresMaterializedEntity)
            {
                requiresMaterializedEntity.Add(mapping.EntityClrType);
            }
            if (mapping.ScopedByTenantId)
            {
                requiresTenantColumn.Add(mapping.EntityClrType);
            }
            if (mapping.DeclaredDependencies.Count > 0)
            {
                if (!declaredDependencies.TryGetValue(mapping.EntityClrType, out var dependencies))
                {
                    declaredDependencies[mapping.EntityClrType] = dependencies = [];
                }
                dependencies.AddRange(mapping.DeclaredDependencies);
            }
            if (mapping.ColumnSelection is { } selection)
            {
                if (!columnSelections.TryGetValue(mapping.EntityClrType, out var selections))
                {
                    columnSelections[mapping.EntityClrType] = selections = [];
                }
                selections.Add(selection);
            }
            else
            {
                consumesAll.Add(mapping.EntityClrType);
            }
        }

        return new CaptureSpec
        {
            DeclaredEntities = declaredEntities,
            RequiresFullReplicaIdentity = requiresFullReplicaIdentity,
            RequiresMaterializedEntity = requiresMaterializedEntity,
            RequiresTenantColumn = requiresTenantColumn,
            DeclaredDependencies = declaredDependencies.ToDictionary(
                d => d.Key, d => (IReadOnlyList<LambdaExpression>)d.Value),
            // A mapping without a selection needs every column, so one such mapping keeps its entity at
            // consume-all regardless of what other mappings declare.
            DeclaredColumnSelections = columnSelections
                .Where(s => !consumesAll.Contains(s.Key))
                .ToDictionary(s => s.Key, s => (IReadOnlyList<ColumnSelection>)s.Value),
        };
    }
}
