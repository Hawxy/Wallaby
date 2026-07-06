using System.Linq.Expressions;
using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Model;
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

    /// <summary>Per-row scope key (e.g. tenant id) for enrichment-session + destination scoping.</summary>
    public Func<ChangeEvent, object?>? ScopeKeySelector { get; set; }

    /// <summary>Per-scope-key destination (e.g. index-per-tenant); falls back to <see cref="Destination"/>.</summary>
    public Func<object?, string?>? DestinationSelector { get; set; }

    /// <summary>
    /// Navigation expressions declared via <c>DependsOn(...)</c>. Each expression points at a single
    /// one-hop navigation whose target/join table should be captured and fan changes out to this
    /// entity. Resolved against the storage provider's model at startup (via
    /// <see cref="IWallabyModelProvider.BuildCapturePlan"/>).
    /// </summary>
    public List<LambdaExpression> DeclaredDependencies { get; } = [];
}

/// <summary>
/// A declared external replication slot — an additional pgoutput publication + slot that Wallaby
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
}

/// <summary>The immutable result of the fluent builder, consumed by the runtime.</summary>
internal sealed class WallabyConfiguration
{
    /// <summary>
    /// Option mutations queued by <see cref="WallabyBuilder.ConfigureOptions"/> and
    /// <see cref="WallabyBuilder.UseConnectionString"/>. Applied to the <see cref="WallabyOptions"/> being built by
    /// the options pipeline at the <c>AddWallaby</c> registration position, so they compose with the standard
    /// <c>Configure&lt;WallabyOptions&gt;</c>/<c>PostConfigure</c> calls in registration order.
    /// </summary>
    public List<Action<WallabyOptions>> OptionsActions { get; } = [];

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
    /// The registered storage providers, in registration order. Empty when no provider is registered
    /// (provision-only). Each provider derives its own capture plan; the plans are merged into one model
    /// sharing a single slot/publication/checkpoint. Names are unique (enforced by
    /// <see cref="WallabyBuilder.UseProvider"/>).
    /// </summary>
    public List<WallabyProviderRegistration> Providers { get; } = [];

    /// <summary>
    /// True when the consumer declared anything that requires the streaming pipeline (a sink; mappings
    /// only exist attached to one). When false, Wallaby runs in provision-only mode: it only creates the
    /// declared external slots and never opens a primary slot or streams.
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
        var declaredDependencies = new Dictionary<Type, List<LambdaExpression>>();
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
            // A scoped destination must resolve the scope key on deletes too, which needs full old-row values.
            if (mapping.DestinationSelector is not null)
            {
                requiresFullReplicaIdentity.Add(mapping.EntityClrType);
            }
            if (mapping.DeclaredDependencies.Count > 0)
            {
                if (!declaredDependencies.TryGetValue(mapping.EntityClrType, out var dependencies))
                {
                    declaredDependencies[mapping.EntityClrType] = dependencies = [];
                }
                dependencies.AddRange(mapping.DeclaredDependencies);
            }
        }

        return new CaptureSpec
        {
            DeclaredEntities = declaredEntities,
            RequiresFullReplicaIdentity = requiresFullReplicaIdentity,
            DeclaredDependencies = declaredDependencies.ToDictionary(
                d => d.Key, d => (IReadOnlyList<LambdaExpression>)d.Value),
        };
    }
}
