using System.Linq.Expressions;
using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.DependencyInjection;

/// <summary>A registered sink: its name and a factory that resolves the instance from the container.</summary>
internal sealed class SinkRegistration
{
    public required string Name { get; init; }
    public required Func<IServiceProvider, ISink> Factory { get; init; }
}

/// <summary>A registered entity→sink mapping plus the factory for its transform invoker.</summary>
internal sealed class MappingRegistration
{
    public required Type EntityClrType { get; init; }
    public string SinkName { get; set; } = "";
    public string? Destination { get; set; }
    public string? BackfillVersion { get; set; }
    public Func<IServiceProvider, IWallabyTransformInvoker>? TransformFactory { get; set; }
    public Func<ChangeEvent, string>? DocumentIdSelector { get; set; }

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

    public bool CaptureAllMapped { get; set; }
    public List<SinkRegistration> Sinks { get; } = [];
    public Dictionary<Type, MappingRegistration> Mappings { get; } = [];
    public HashSet<Type> RequiresFullReplicaIdentity { get; } = [];

    /// <summary>External pgoutput publication+slot pairs to provision for third-party consumers (e.g. ELT).</summary>
    public List<ExternalSlotRegistration> ExternalSlots { get; } = [];

    /// <summary>
    /// Builds the <see cref="ITransactionSpill"/> that buffers a pgoutput v2 streamed (large) transaction until
    /// commit. Set by <c>SpillToDisk</c>/<c>SpillToDatabase</c>/<c>UseTransactionSpill</c>; null selects the
    /// default database-backed spill. Invoked once per leader session with the runtime's <see cref="SpillContext"/>.
    /// </summary>
    public Func<SpillContext, ITransactionSpill>? SpillFactory { get; set; }

    /// <summary>The registered storage provider's display name (e.g. <c>"EntityFrameworkCore"</c>), for error messages.</summary>
    public string? ProviderName { get; set; }

    /// <summary>
    /// Builds the storage provider's model provider. Set by <see cref="WallabyBuilder.UseProvider"/>; null when
    /// no provider is registered (provision-only). Used to resolve the capture plan and
    /// <c>ForEntity&lt;T&gt;()</c> external-slot table declarations.
    /// </summary>
    public Func<IServiceProvider, IWallabyModelProvider>? ModelProvider { get; set; }

    /// <summary>
    /// Builds the default (unscoped) enrichment-session provider transforms lease their sessions from.
    /// Set by <see cref="WallabyBuilder.UseProvider"/>.
    /// </summary>
    public Func<IServiceProvider, IEnrichmentSessionProvider>? EnrichmentSessions { get; set; }

    /// <summary>
    /// Optional scope-key-aware enrichment-session provider (e.g. context-per-tenant); overrides
    /// <see cref="EnrichmentSessions"/> when set. Set via <see cref="WallabyBuilder.UseScopedEnrichmentSessions"/>.
    /// </summary>
    public Func<IServiceProvider, IEnrichmentSessionProvider>? ScopedEnrichmentSessions { get; set; }

    /// <summary>
    /// True when the consumer declared anything that requires the streaming pipeline (a sink, a mapping, or
    /// <c>CaptureAllMappedTables()</c>). When false, Wallaby runs in provision-only mode: it only creates the
    /// declared external slots and never opens a primary slot or streams.
    /// </summary>
    public bool CaptureIntended => Sinks.Count > 0 || Mappings.Count > 0 || CaptureAllMapped;

    /// <summary>
    /// Build the <see cref="CaptureSpec"/> the storage provider consumes, including each mapping's
    /// <c>DependsOn(...)</c> navigations. Shared by the runtime and the backfill-manager registration so both
    /// derive the same <see cref="WallabyModel"/> (dependent tables/bindings included).
    /// </summary>
    public CaptureSpec ToCaptureSpec()
    {
        var declaredDependencies = new Dictionary<Type, IReadOnlyList<LambdaExpression>>();
        foreach (var mapping in Mappings.Values)
        {
            if (mapping.DeclaredDependencies.Count > 0)
            {
                declaredDependencies[mapping.EntityClrType] = mapping.DeclaredDependencies;
            }
        }

        return new CaptureSpec
        {
            CaptureAllMapped = CaptureAllMapped,
            DeclaredEntities = [.. Mappings.Keys],
            RequiresFullReplicaIdentity = RequiresFullReplicaIdentity,
            DeclaredDependencies = declaredDependencies,
        };
    }
}
