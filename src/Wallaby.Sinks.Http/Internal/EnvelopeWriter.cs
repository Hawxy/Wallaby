using System.Buffers;
using System.Text.Json;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Http.Internal;

/// <summary>
/// Serializes a slice of a <see cref="SinkBatch"/> into the sink's JSON envelope. The record-level
/// pieces (metadata, document values, idempotency key) come from <see cref="SinkEnvelopeJson"/>;
/// this class owns the batch envelope shape.
/// </summary>
internal static class EnvelopeWriter
{
    private const string SerializerOptionsName = "HttpSinkOptions.SerializerOptions";

    /// <summary>
    /// Write records <paramref name="offset"/>..<paramref name="offset"/>+<paramref name="count"/> as one
    /// envelope into <paramref name="buffer"/> (the caller owns and may reuse it between calls).
    /// </summary>
    public static void Write(
        ArrayBufferWriter<byte> buffer,
        string sinkName,
        IReadOnlyList<SinkRecord> records,
        int offset,
        int count,
        IReadOnlyDictionary<string, string>? annotations,
        JsonSerializerOptions? serializerOptions)
    {
        using var writer = new Utf8JsonWriter(buffer);

        writer.WriteStartObject();
        writer.WriteString("type", "wallaby.changes");
        writer.WriteString("sink", sinkName);
        writer.WriteString("sentAt", DateTimeOffset.UtcNow);
        if (annotations is { Count: > 0 })
        {
            writer.WriteStartObject("annotations");
            foreach (var annotation in annotations)
            {
                writer.WriteString(annotation.Key, annotation.Value);
            }
            writer.WriteEndObject();
        }
        writer.WriteStartArray("records");
        for (var i = offset; i < offset + count; i++)
        {
            WriteRecord(writer, records[i], serializerOptions);
        }
        writer.WriteEndArray();
        writer.WriteEndObject();

        writer.Flush();
    }

    private static void WriteRecord(Utf8JsonWriter writer, SinkRecord record, JsonSerializerOptions? serializerOptions)
    {
        writer.WriteStartObject();
        writer.WriteString("operation", record.IsDeletion ? "delete" : "upsert");
        writer.WriteString("id", record.DocumentId);
        writer.WriteString("idempotencyKey", SinkEnvelopeJson.IdempotencyKey(record));
        if (record.Destination is null)
        {
            writer.WriteNull("destination");
        }
        else
        {
            writer.WriteString("destination", record.Destination);
        }

        if (!record.IsDeletion)
        {
            writer.WritePropertyName("document");
            SinkEnvelopeJson.WriteDocument(writer, record.Document!, record.DocumentId, serializerOptions, SerializerOptionsName);
        }

        SinkEnvelopeJson.WriteMetadata(writer, record.Metadata);
        writer.WriteEndObject();
    }
}
