using EFCore.CDC.Abstractions;

namespace EFCore.CDC.Internal.Pipeline;

/// <summary>
/// A routing rule binding an entity type to a sink/destination (and a transform that shapes the document).
/// Routing concerns only — the transform owns the data shaping.
/// </summary>
internal sealed class EntityMapping
{
    public required Type EntityClrType { get; init; }
    public required string SinkName { get; init; }
    public string? Destination { get; init; }
    public string? BackfillVersion { get; init; }
    public required ITransformInvoker Transform { get; init; }

    /// <summary>Optional custom document-id rule; defaults to the source primary key.</summary>
    public Func<ChangeEvent, string>? DocumentIdSelector { get; init; }

    /// <summary>
    /// Optional per-row scope key (e.g. tenant id) derived from a change. Drives the enrichment
    /// <c>DbContext</c> chosen for the transform and (when set) the destination. Null when not scoped.
    /// </summary>
    public Func<ChangeEvent, object?>? ScopeKeySelector { get; init; }

    /// <summary>Optional destination computed from the scope key; falls back to <see cref="Destination"/>.</summary>
    public Func<object?, string?>? DestinationSelector { get; init; }

    public string GetDocumentId(ChangeEvent change)
        => DocumentIdSelector?.Invoke(change) ?? change.Key.ToString();

    /// <summary>The scope key for a change (null when the mapping is not scoped, or the entity is unavailable).</summary>
    public object? GetScopeKey(ChangeEvent change)
        => ScopeKeySelector is null || change.Entity is null ? null : ScopeKeySelector(change);

    /// <summary>The destination for a change's scope key (scoped selector wins, else the fixed destination).</summary>
    public string? ResolveDestination(object? scopeKey)
        => DestinationSelector is not null ? DestinationSelector(scopeKey) : Destination;
}
