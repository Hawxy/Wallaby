using System.Text.Json;
using System.Text.Json.Serialization;
using Wallaby.Sinks;

namespace Wallaby.Sinks.Elasticsearch;

/// <summary>
/// Bridges a record's dynamic field bag into <c>Elastic.Ingest.Elasticsearch</c>'s <c>BulkSender</c>, which
/// requires a static <see cref="System.Text.Json.Serialization.Metadata.JsonTypeInfo{T}"/> per document type.
/// Carries <see cref="DocumentId"/> alongside the document because <see cref="BulkDocumentBodyConverter"/>'s
/// <see cref="JsonConverter{T}.Write"/> only receives the value being serialized, not the originating record.
/// </summary>
internal readonly record struct BulkDocumentBody(string DocumentId, IReadOnlyDictionary<string, object?> Document);

/// <summary>
/// Writes a <see cref="BulkDocumentBody"/> by delegating to
/// <see cref="SinkEnvelopeJson.WriteDocument(Utf8JsonWriter, IReadOnlyDictionary{string, object?}, string, JsonSerializerOptions?)"/>,
/// the same reflection-free (AOT-safe) writer the HTTP and Kafka sinks use. This is the only place
/// <c>Wallaby.Sinks.Elasticsearch</c> implements <see cref="JsonConverter{T}"/>: it exists solely to give
/// <c>BulkSender</c>'s <see cref="System.Text.Json.Serialization.Metadata.JsonTypeInfo{T}"/> contract a document
/// shape to point at, the actual field-by-field writing is unchanged.
/// </summary>
internal sealed class BulkDocumentBodyConverter(JsonSerializerOptions? serializerOptions) : JsonConverter<BulkDocumentBody>
{
    public override void Write(Utf8JsonWriter writer, BulkDocumentBody value, JsonSerializerOptions options) =>
        SinkEnvelopeJson.WriteDocument(writer, value.Document, value.DocumentId, serializerOptions);

    public override BulkDocumentBody Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        throw new NotSupportedException($"{nameof(BulkDocumentBody)} is write-only: it is never deserialized.");
}
