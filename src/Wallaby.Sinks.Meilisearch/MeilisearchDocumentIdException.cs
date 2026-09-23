namespace Wallaby.Sinks.Meilisearch;

/// <summary>
/// Thrown when a document id exceeds Meilisearch's id length limit once encoded. Retrying can never succeed,
/// so the sink reports it as a permanent delivery failure.
/// </summary>
public sealed class MeilisearchDocumentIdException(string documentId, string message) : Exception(message)
{
    /// <summary>The canonical id of the offending document.</summary>
    public string DocumentId { get; } = documentId;
}
