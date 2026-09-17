namespace Wallaby.Sinks.Meilisearch;

/// <summary>A document value the JSON writer cannot encode; delivery fails permanently.</summary>
internal sealed class MeilisearchSerializationException(Exception inner)
    : Exception($"Meilisearch document serialization failed: {inner.Message}", inner);
