using System.Linq.Expressions;

namespace EFCore.CDC.Internal.SelfConfig;

/// <summary>
/// Declares which EF Core entities to capture. Either every mapped entity (<see cref="CaptureAllMapped"/>)
/// or an explicit set (<see cref="DeclaredEntities"/>). Entities listed in
/// <see cref="RequiresFullReplicaIdentity"/> are flagged so self-config can validate <c>REPLICA IDENTITY</c>.
/// </summary>
internal sealed class CaptureSpec
{
    /// <summary>When true, capture every mapped, keyed, table-backed entity in the model.</summary>
    public bool CaptureAllMapped { get; init; }

    /// <summary>The explicitly declared entity types to capture (used when <see cref="CaptureAllMapped"/> is false).</summary>
    public IReadOnlyList<Type> DeclaredEntities { get; init; } = [];

    /// <summary>Entity types that need <c>REPLICA IDENTITY FULL</c> (a transform reads old values / full row).</summary>
    public IReadOnlySet<Type> RequiresFullReplicaIdentity { get; init; } = new HashSet<Type>();

    /// <summary>
    /// Per-primary-entity navigation expressions declared via <c>DependsOn(...)</c>. Each entry's key
    /// is the primary CLR type; the values are <c>Expression&lt;Func&lt;TEntity, TNav&gt;&gt;</c> lambdas
    /// resolved against the EF Core model at startup to produce dependent-table captures and fan-out
    /// bindings.
    /// </summary>
    public IReadOnlyDictionary<Type, IReadOnlyList<LambdaExpression>> DeclaredDependencies { get; init; }
        = new Dictionary<Type, IReadOnlyList<LambdaExpression>>();
}
