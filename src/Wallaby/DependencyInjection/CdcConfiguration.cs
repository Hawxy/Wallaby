using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.SelfConfig;

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
    public Func<IServiceProvider, ITransformInvoker>? TransformFactory { get; set; }
    public Func<ChangeEvent, string>? DocumentIdSelector { get; set; }

    /// <summary>Per-row scope key (e.g. tenant id) for enrichment-context + destination scoping.</summary>
    public Func<ChangeEvent, object?>? ScopeKeySelector { get; set; }

    /// <summary>Per-scope-key destination (e.g. index-per-tenant); falls back to <see cref="Destination"/>.</summary>
    public Func<object?, string?>? DestinationSelector { get; set; }

    /// <summary>
    /// Navigation expressions declared via <c>DependsOn(...)</c>. Each expression points at a single
    /// EF Core navigation (reference, collection, or skip-navigation) whose target/join table should
    /// be captured and fan changes out to this entity. Resolved against the EF Core <c>IModel</c> at
    /// startup by <c>DependencyAnalyzer</c>.
    /// </summary>
    public List<LambdaExpression> DeclaredDependencies { get; } = [];
}

/// <summary>
/// A declared external replication slot — an additional pgoutput publication + slot that Wallaby
/// provisions (and reconciles) for a third-party CDC consumer (e.g. an ELT tool) but never consumes.
/// Table declarations are resolved to schema-qualified names against the EF Core model at startup.
/// </summary>
internal sealed class ExternalSlotRegistration
{
    public required string SlotName { get; init; }

    /// <summary>Optional publication name; defaults to <c>"{SlotName}_pub"</c> when unset.</summary>
    public string? PublicationName { get; set; }

    /// <summary>Tables declared by schema-qualified name.</summary>
    public List<(string Schema, string Table)> TableNames { get; } = [];

    /// <summary>Tables declared by entity CLR type, resolved against the EF Core model at startup.</summary>
    public List<Type> EntityTypes { get; } = [];
}

/// <summary>The immutable result of the fluent builder, consumed by the runtime.</summary>
internal sealed class CdcConfiguration
{
    public required CdcOptions Options { get; init; }
    public bool CaptureAllMapped { get; set; }
    public List<SinkRegistration> Sinks { get; } = [];
    public Dictionary<Type, MappingRegistration> Mappings { get; } = [];
    public HashSet<Type> RequiresFullReplicaIdentity { get; } = [];

    /// <summary>External pgoutput publication+slot pairs to provision for third-party consumers (e.g. ELT).</summary>
    public List<ExternalSlotRegistration> ExternalSlots { get; } = [];

    /// <summary>Optional factory that builds the enrichment <see cref="DbContext"/> from a row's scope key.</summary>
    public Func<object?, IServiceProvider, DbContext>? ScopedContextFactory { get; set; }

    /// <summary>
    /// Builds the <see cref="ITransactionSpill"/> that buffers a pgoutput v2 streamed (large) transaction until
    /// commit. Set by <c>SpillToDisk</c>/<c>SpillToDatabase</c>/<c>UseTransactionSpill</c>; null selects the
    /// default database-backed spill. Invoked once per leader session with the runtime's <see cref="SpillContext"/>.
    /// </summary>
    public Func<SpillContext, ITransactionSpill>? SpillFactory { get; set; }

    /// <summary>
    /// Reads the EF Core <see cref="IModel"/> from the declared capture context. Set by
    /// <see cref="CdcBuilder.UseContext{TContext}"/>; null when no context is declared (provision-only).
    /// Used to resolve <c>ForEntity&lt;T&gt;()</c> external-slot table declarations.
    /// </summary>
    public Func<IServiceProvider, IModel>? ModelAccessor { get; set; }

    /// <summary>
    /// Leases an enrichment <see cref="DbContext"/> for the runtime, using a registered
    /// <see cref="IDbContextFactory{TContext}"/> when present and otherwise a DI scope over the consumer's
    /// <c>AddDbContext</c> registration. Set by <see cref="CdcBuilder.UseContext{TContext}"/>.
    /// </summary>
    public Func<IServiceProvider, EnrichmentContextLease>? ContextLease { get; set; }

    /// <summary>
    /// True when the consumer declared anything that requires the streaming pipeline (a sink, a mapping, or
    /// <c>CaptureAllMappedTables()</c>). When false, Wallaby runs in provision-only mode: it only creates the
    /// declared external slots and never opens a primary slot or streams.
    /// </summary>
    public bool CaptureIntended => Sinks.Count > 0 || Mappings.Count > 0 || CaptureAllMapped;

    /// <summary>
    /// Build the <see cref="CaptureSpec"/> the model resolver consumes, including each mapping's
    /// <c>DependsOn(...)</c> navigations. Shared by the runtime and the backfill-manager registration so both
    /// derive the same <see cref="Wallaby.Model.CdcModel"/> (dependent tables/bindings included).
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
