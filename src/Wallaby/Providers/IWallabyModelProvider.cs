namespace Wallaby.Providers;

/// <summary>
/// The storage-provider seam that turns the consumer's declared model (e.g. an EF Core <c>IModel</c> or a
/// Marten document store) into Wallaby's capture plan. Registered on the builder by a provider package
/// (e.g. <c>UseEntityFrameworkCore&lt;TContext&gt;()</c>) via <c>UseProvider(...)</c>.
/// </summary>
public interface IWallabyModelProvider
{
    /// <summary>
    /// Resolve the capture plan — tables, keys, dependent-table bindings, and the row materializer —
    /// once at startup. Declared <c>DependsOn(...)</c> expressions in <paramref name="spec"/> are resolved
    /// against the provider's model here.
    /// </summary>
    CapturePlan BuildCapturePlan(CaptureSpec spec);

    /// <summary>Resolve an entity type declared via <c>AddExternalSlot(...).ForEntity&lt;T&gt;()</c> to its table.</summary>
    QualifiedTable ResolveTable(Type entityClrType);

    /// <summary>
    /// True when this provider's model maps <paramref name="entityClrType"/> to a capturable table.
    /// With multiple registered providers, each mapping is assigned to the provider that claims its
    /// type here (ambiguity or no claimant fails startup).
    /// </summary>
    bool Handles(Type entityClrType);
}
