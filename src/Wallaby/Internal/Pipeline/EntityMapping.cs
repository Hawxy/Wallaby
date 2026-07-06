using Wallaby.Abstractions;
using Wallaby.Providers;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// A routing rule binding an entity type to a sink/destination (and a transform that shapes the document).
/// Routing concerns only — the transform owns the data shaping. A record so test infrastructure can
/// late-bind <see cref="Sessions"/> via <c>with</c>.
/// </summary>
internal sealed record EntityMapping
{
    public required Type EntityClrType { get; init; }
    public required string SinkName { get; init; }
    public string? Destination { get; init; }
    public required IWallabyTransformInvoker Transform { get; init; }

    /// <summary>
    /// The enrichment sessions this mapping's transform leases (the mapping's provider's, scoped override
    /// first). Mappings on the same provider share a session per batch; mappings on different providers
    /// lease independently.
    /// </summary>
    public required IEnrichmentSessionProvider Sessions { get; init; }

    /// <summary>Optional custom document-id rule; defaults to the source primary key.</summary>
    public Func<ChangeEvent, string>? DocumentIdSelector { get; init; }

    /// <summary>
    /// Optional per-row scope key (e.g. tenant id) derived from a change. Drives the enrichment
    /// session chosen for the transform and (when set) the destination. Null when not scoped.
    /// </summary>
    public Func<ChangeEvent, object?>? ScopeKeySelector { get; init; }

    /// <summary>Optional destination computed from the scope key; falls back to <see cref="Destination"/>.</summary>
    public Func<object?, string?>? DestinationSelector { get; init; }

    public string GetDocumentId(ChangeEvent change)
        => DocumentIdSelector?.Invoke(change) ?? change.Key.ToString();

    /// <summary>
    /// The scope key for a change (null when the mapping is not scoped). Runs for deletes too — a
    /// record-based selector resolves the key from captured (old-row) values even when no entity was
    /// materialized; the entity-typed <c>ScopedBy</c> overload guards its own entity access.
    /// </summary>
    public object? GetScopeKey(ChangeEvent change)
        => ScopeKeySelector?.Invoke(change);

    /// <summary>The destination for a change's scope key (scoped selector wins, else the fixed destination).</summary>
    public string? ResolveDestination(object? scopeKey)
        => DestinationSelector is not null ? DestinationSelector(scopeKey) : Destination;
}
