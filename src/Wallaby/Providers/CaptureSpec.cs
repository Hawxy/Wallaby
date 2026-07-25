using System.Linq.Expressions;

namespace Wallaby.Providers;

/// <summary>
/// Declares which entities to capture (<see cref="DeclaredEntities"/>). Entities listed in
/// <see cref="RequiresFullReplicaIdentity"/> are flagged so self-config can validate <c>REPLICA IDENTITY</c>.
/// The storage provider resolves this spec into a <see cref="CapturePlan"/> at startup.
/// </summary>
public sealed class CaptureSpec
{
    /// <summary>The declared entity types to capture.</summary>
    public IReadOnlyList<Type> DeclaredEntities { get; init; } = [];

    /// <summary>Entity types that need <c>REPLICA IDENTITY FULL</c> (a transform reads old values / full row).</summary>
    public IReadOnlySet<Type> RequiresFullReplicaIdentity { get; init; } = new HashSet<Type>();

    /// <summary>
    /// Entity types whose delete-time identity or routing is computed from the materialized entity
    /// (<c>KeyedBy</c>, entity-typed <c>ScopedBy</c> with a <c>ScopedDestination</c>). Always a subset of
    /// <see cref="RequiresFullReplicaIdentity"/>; a missing replica identity is an error for these rather
    /// than a warning, because deletes would target the wrong document or destination.
    /// </summary>
    public IReadOnlySet<Type> RequiresMaterializedEntity { get; init; } = new HashSet<Type>();

    /// <summary>
    /// Per-primary-entity navigation expressions declared via <c>DependsOn(...)</c>. Each entry's key
    /// is the primary CLR type; the values are <c>Expression&lt;Func&lt;TEntity, TNav&gt;&gt;</c> lambdas
    /// the storage provider resolves against its model at startup to produce dependent-table captures
    /// and fan-out bindings.
    /// </summary>
    public IReadOnlyDictionary<Type, IReadOnlyList<LambdaExpression>> DeclaredDependencies { get; init; }
        = new Dictionary<Type, IReadOnlyList<LambdaExpression>>();

    /// <summary>
    /// Per-entity column selections declared via provider mapping extensions (e.g. EF Core's
    /// <c>Consumes</c>/<c>ConsumesAllExcept</c>). The entity's captured column set is the union of its
    /// selections; entities absent here are captured whole (an entity with any selection-less mapping is
    /// omitted by <c>WallabyConfiguration.ToCaptureSpec</c>).
    /// </summary>
    public IReadOnlyDictionary<Type, IReadOnlyList<ColumnSelection>> DeclaredColumnSelections { get; init; }
        = new Dictionary<Type, IReadOnlyList<ColumnSelection>>();
}
