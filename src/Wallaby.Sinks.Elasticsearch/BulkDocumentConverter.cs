using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Elasticsearch;

/// <summary>
/// Writes a record's bulk document line through the reflection-free (AOT-safe)
/// <see cref="SinkEnvelopeJson.WriteDocument(Utf8JsonWriter, IReadOnlyDictionary{string, object?}, string, JsonSerializerOptions?)"/>,
/// giving <c>BulkSender</c> the <see cref="JsonTypeInfo{T}"/> it requires for the body type.
/// </summary>
internal sealed class BulkDocumentConverter(JsonSerializerOptions? serializerOptions) : JsonConverter<SinkRecord>
{
    /// <summary>The converter-backed type info the sink hands to <c>BulkSender</c>.</summary>
    public static JsonTypeInfo<SinkRecord> CreateTypeInfo(JsonSerializerOptions? serializerOptions)
    {
        // STJ requires a non-null resolver even though the converter makes it unused; an empty Combine() is reflection-free.
        var options = new JsonSerializerOptions { TypeInfoResolver = JsonTypeInfoResolver.Combine() };
        var converter = new BulkDocumentConverter(serializerOptions);
        return JsonMetadataServices.CreateValueInfo<SinkRecord>(options, converter);
    }

    public override void Write(Utf8JsonWriter writer, SinkRecord value, JsonSerializerOptions options) =>
        SinkEnvelopeJson.WriteDocument(writer, value.Document!, value.DocumentId, serializerOptions);

    public override SinkRecord Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        throw new NotSupportedException("Bulk documents are write-only: they are never deserialized.");
}
