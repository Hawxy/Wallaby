namespace Wallaby.Sinks.Meilisearch;

/// <summary>
/// Thrown when <see cref="MeilisearchSinkOptions.ValidateConfiguredAttributes"/> is enabled and an upsert
/// document is missing a key for an attribute the target index was configured with (searchable, filterable, or
/// sortable). It signals a configuration/transform mismatch — a non-retryable error — so the sink reports it as
/// a permanent delivery failure rather than retrying.
/// </summary>
public sealed class MeilisearchDocumentValidationException(
    string index, string documentId, IReadOnlyCollection<string> missingAttributes)
    : Exception(
        $"Document '{documentId}' routed to Meilisearch index '{index}' is missing configured attribute(s) " +
        $"[{string.Join(", ", missingAttributes)}]. The transform must emit these keys, or they must be removed " +
        "from the index configuration.")
{
    /// <summary>The index the document was routed to.</summary>
    public string Index { get; } = index;

    /// <summary>The id of the offending document.</summary>
    public string DocumentId { get; } = documentId;

    /// <summary>Configured attributes that were absent from the document.</summary>
    public IReadOnlyCollection<string> MissingAttributes { get; } = missingAttributes;
}
