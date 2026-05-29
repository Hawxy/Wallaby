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

    public string GetDocumentId(ChangeEvent change)
        => DocumentIdSelector?.Invoke(change) ?? new DocumentKey(change.PrimaryKey).ToString();
}
