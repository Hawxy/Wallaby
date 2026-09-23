namespace Wallaby.Sinks.Meilisearch;

/// <summary>
/// Thrown when a document cannot be stored under its id: the encoded id exceeds Meilisearch's limit, or the
/// document carries a <see cref="MeilisearchSinkOptions.PrimaryKey"/> field that differs from its id. Either is
/// a configuration/transform error, so the sink reports it as a permanent delivery failure.
/// </summary>
public sealed class MeilisearchDocumentIdException(string documentId, string message) : Exception(message)
{
    /// <summary>The canonical id of the offending document.</summary>
    public string DocumentId { get; } = documentId;
}
