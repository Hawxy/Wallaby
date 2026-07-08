using System.Buffers;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.OpenSearch.Internal;

/// <summary>
/// Serializes a slice of a <see cref="SinkBatch"/> into an NDJSON <c>_bulk</c> body: an action line
/// (<c>index</c>/<c>delete</c> with <c>_index</c> and <c>_id</c>) followed by the document line for upserts.
/// Common scalar values are written directly with <see cref="Utf8JsonWriter"/> (reflection-free); other
/// value types go through the consumer's <see cref="JsonSerializerOptions"/>, falling back to
/// reflection-based serialization only where the host supports it.
/// </summary>
internal static class BulkWriter
{
    private static readonly byte[] NewLine = "\n"u8.ToArray();

    /// <summary>Write records <paramref name="offset"/>..<paramref name="offset"/>+<paramref name="count"/> as one bulk body.</summary>
    public static byte[] Write(
        string sinkName,
        IReadOnlyList<SinkRecord> records,
        int offset,
        int count,
        string? defaultIndex,
        JsonSerializerOptions? serializerOptions)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer);

        for (var i = offset; i < offset + count; i++)
        {
            var record = records[i];
            var index = record.Destination ?? defaultIndex
                ?? throw new InvalidOperationException(
                    $"Record {record.DocumentId} has no destination and no DefaultIndex is configured for sink '{sinkName}'.");

            WriteAction(writer, buffer, record, index);
            if (!record.IsDeletion)
            {
                WriteDocument(writer, record.Document!, record.DocumentId, serializerOptions);
                EndLine(writer, buffer);
            }
        }

        return buffer.WrittenSpan.ToArray();
    }

    private static void WriteAction(Utf8JsonWriter writer, ArrayBufferWriter<byte> buffer, SinkRecord record, string index)
    {
        writer.WriteStartObject();
        writer.WriteStartObject(record.IsDeletion ? "delete" : "index");
        writer.WriteString("_index", index);
        writer.WriteString("_id", record.DocumentId);
        writer.WriteEndObject();
        writer.WriteEndObject();
        EndLine(writer, buffer);
    }

    /// <summary>Commit the current JSON line, append the NDJSON newline, and reset for the next line.</summary>
    private static void EndLine(Utf8JsonWriter writer, ArrayBufferWriter<byte> buffer)
    {
        writer.Flush();
        buffer.Write(NewLine);
        writer.Reset();
    }

    private static void WriteDocument(Utf8JsonWriter writer, IReadOnlyDictionary<string, object?> document,
        string documentId, JsonSerializerOptions? serializerOptions)
    {
        writer.WriteStartObject();
        foreach (var field in document)
        {
            writer.WritePropertyName(field.Key);
            WriteValue(writer, field.Value, field.Key, documentId, serializerOptions);
        }
        writer.WriteEndObject();
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value, string key, string documentId,
        JsonSerializerOptions? serializerOptions)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case string s: writer.WriteStringValue(s); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case short sh: writer.WriteNumberValue(sh); break;
            case byte by: writer.WriteNumberValue(by); break;
            case sbyte sb: writer.WriteNumberValue(sb); break;
            case ushort us: writer.WriteNumberValue(us); break;
            case uint ui: writer.WriteNumberValue(ui); break;
            case ulong ul: writer.WriteNumberValue(ul); break;
            case float f: writer.WriteNumberValue(f); break;
            case double d: writer.WriteNumberValue(d); break;
            case decimal m: writer.WriteNumberValue(m); break;
            case Guid g: writer.WriteStringValue(g); break;
            case DateTime dt: writer.WriteStringValue(dt); break;
            case DateTimeOffset dto: writer.WriteStringValue(dto); break;
            case DateOnly date: writer.WriteStringValue(date.ToString("O", CultureInfo.InvariantCulture)); break;
            case TimeOnly time: writer.WriteStringValue(time.ToString("O", CultureInfo.InvariantCulture)); break;
            case TimeSpan span: writer.WriteStringValue(span.ToString(null, CultureInfo.InvariantCulture)); break;
            case char c: writer.WriteStringValue(c.ToString()); break;
            case byte[] bytes: writer.WriteBase64StringValue(bytes); break;
            case Uri uri: writer.WriteStringValue(uri.ToString()); break;
            case IReadOnlyDictionary<string, object?> nested:
                WriteDocument(writer, nested, documentId, serializerOptions);
                break;
            case IEnumerable sequence:
                writer.WriteStartArray();
                foreach (var item in sequence)
                {
                    WriteValue(writer, item, key, documentId, serializerOptions);
                }
                writer.WriteEndArray();
                break;
            default:
                WriteFallback(writer, value, key, documentId, serializerOptions);
                break;
        }
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Consumer-supplied SerializerOptions resolve types through their own TypeInfoResolver " +
                        "(source-generated on AOT); the reflection default only runs when IsReflectionEnabledByDefault is true.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Consumer-supplied SerializerOptions resolve types through their own TypeInfoResolver " +
                        "(source-generated on AOT); the reflection default only runs when IsReflectionEnabledByDefault is true.")]
    private static void WriteFallback(Utf8JsonWriter writer, object value, string key, string documentId,
        JsonSerializerOptions? serializerOptions)
    {
        if (serializerOptions is null && !JsonSerializer.IsReflectionEnabledByDefault)
        {
            throw new NotSupportedException(
                $"Document '{documentId}' field '{key}' has type '{value.GetType()}', which has no built-in JSON " +
                "encoding, and reflection-based serialization is disabled (trimmed/NativeAOT host). Set " +
                "OpenSearchSinkOptions.SerializerOptions with a source-generated JsonSerializerContext covering the type.");
        }

        JsonSerializer.Serialize(writer, value, value.GetType(), serializerOptions ?? JsonSerializerOptions.Default);
    }
}
