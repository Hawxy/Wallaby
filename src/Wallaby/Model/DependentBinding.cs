namespace Wallaby.Model;

/// <summary>
/// One lookup column in a <see cref="DependentBinding"/>: read <see cref="DependentColumn"/> from the
/// changed row in the dependent table, then match it against <see cref="PrimaryColumn"/> when re-reading
/// the primary table to find the affected primary keys.
/// </summary>
public sealed record DependentLookupColumn(string DependentColumn, string PrimaryColumn);

/// <summary>
/// A rule that fans a change in one table (the <see cref="DependentTable"/>) out to one or more
/// synthetic update events for a different mapped entity (the <see cref="PrimaryTable"/>).
/// </summary>
/// <remarks>
/// Two shapes are supported:
/// <list type="bullet">
///   <item><b>Reference navigation</b> (e.g. <c>Product.Category</c>): the dependent table is the
///   principal (<c>categories</c>); the lookup matches <c>categories.id</c> against the dependent-side
///   FK column on the primary (<c>products.category_id</c>).</item>
///   <item><b>Skip-navigation</b> (e.g. <c>Product.Tags</c>): the dependent table is the join
///   (<c>product_tags</c>); the lookup matches the join's FK column directly against the primary PK
///   (<c>product_tags.product_id</c> → <c>products.id</c>).</item>
/// </list>
/// Owned-entity side tables follow the same shape as the join-table case.
/// </remarks>
public sealed class DependentBinding
{
    public required CapturedTable PrimaryTable { get; init; }
    public required CapturedTable DependentTable { get; init; }
    public required IReadOnlyList<DependentLookupColumn> Lookup { get; init; }
}
