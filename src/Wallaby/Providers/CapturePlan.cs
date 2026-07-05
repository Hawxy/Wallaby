using Wallaby.Model;

namespace Wallaby.Providers;

/// <summary>
/// The storage provider's startup product: the derived capture model (tables, keys, fan-out bindings)
/// plus the row materializer bound to it. Resolved once and shared by the runtime and the backfill manager.
/// </summary>
public sealed class CapturePlan
{
    /// <summary>The capture model: tables, columns, primary keys, and dependent-table bindings.</summary>
    public required WallabyModel Model { get; init; }

    /// <summary>The materializer that turns decoded changes into CLR entities for <see cref="Model"/>.</summary>
    public required IRowMaterializer Materializer { get; init; }
}
