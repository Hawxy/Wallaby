using System.Buffers;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Kafka.Internal;

/// <summary>
/// Serializes one <see cref="SinkRecord"/> into a Kafka message value (a self-contained JSON envelope)
/// and its headers. The envelope structure and common scalar values are written directly with
/// <see cref="Utf8JsonWriter"/> (reflection-free); other value types go through the consumer's
/// <see cref="JsonSerializerOptions"/>, falling back to reflection-based serialization only where the
/// host supports it. Deletions carry no value (a tombstone), so their context lives in the headers.
/// </summary>
internal static class KafkaMessageWriter
{
    /// <summary>Header carrying <c>upsert</c> or <c>delete</c>.</summary>
    public const string OperationHeader = "wallaby.operation";

    /// <summary>Header carrying the per-change deduplication key (see <see cref="IdempotencyKey"/>).</summary>
    public const string IdempotencyKeyHeader = "wallaby.idempotency-key";

    /// <summary>Header carrying the schema-qualified source table.</summary>
    public const string TableHeader = "wallaby.table";

    /// <summary>Header carrying the commit LSN (decimal string; <c>0</c> for backfill reads).</summary>
    public const string CommitLsnHeader = "wallaby.commit-lsn";

    /// <summary>Headers for any record — present on tombstones too, where they are the only metadata.</summary>
    public static Headers BuildHeaders(SinkRecord record) => new()
    {
        { OperationHeader, Encoding.UTF8.GetBytes(record.IsDeletion ? "delete" : "upsert") },
        { IdempotencyKeyHeader, Encoding.UTF8.GetBytes(IdempotencyKey(record)) },
        { TableHeader, Encoding.UTF8.GetBytes(record.Metadata.QualifiedTableName) },
        { CommitLsnHeader, Encoding.UTF8.GetBytes(record.Metadata.CommitLsn.ToString(CultureInfo.InvariantCulture)) },
    };

    /// <summary>Write an upsert's message value.</summary>
    public static byte[] WriteValue(
        SinkRecord record,
        IReadOnlyDictionary<string, string>? annotations,
        JsonSerializerOptions? serializerOptions)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer);

        writer.WriteStartObject();
        writer.WriteString("operation", "upsert");
        writer.WriteString("id", record.DocumentId);
        writer.WriteString("idempotencyKey", IdempotencyKey(record));
        if (annotations is { Count: > 0 })
        {
            writer.WriteStartObject("annotations");
            foreach (var annotation in annotations)
            {
                writer.WriteString(annotation.Key, annotation.Value);
            }
            writer.WriteEndObject();
        }

        writer.WritePropertyName("document");
        WriteDocument(writer, record.Document!, record.DocumentId, serializerOptions);

        WriteMetadata(writer, record.Metadata);
        writer.WriteEndObject();

        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>
    /// An opaque per-record deduplication key. Unique per delivered change; separate backfill runs of the
    /// same row intentionally share a key (backfill is upsert-only, so replays are harmless).
    /// </summary>
    public static string IdempotencyKey(SinkRecord record)
    {
        var metadata = record.Metadata;
        var scope = record.Destination ?? metadata.QualifiedTableName;
        return metadata.IsBackfill
            ? $"backfill:{scope}:{record.DocumentId}"
            : $"{metadata.CommitLsn}:{metadata.CommitIdx}:{scope}:{record.DocumentId}";
    }

    private static void WriteMetadata(Utf8JsonWriter writer, ChangeMetadata metadata)
    {
        writer.WriteStartObject("metadata");
        writer.WriteString("schema", metadata.TableSchema);
        writer.WriteString("table", metadata.TableName);
        writer.WriteString("action", metadata.Action switch
        {
            ChangeAction.Insert => "insert",
            ChangeAction.Update => "update",
            ChangeAction.Delete => "delete",
            _ => "read",
        });
        // As a string: the ulong LSN can exceed the safe-integer range of JavaScript consumers.
        writer.WriteString("commitLsn", metadata.CommitLsn.ToString(CultureInfo.InvariantCulture));
        writer.WriteNumber("commitIdx", metadata.CommitIdx);
        if (metadata.CommitTimestamp is { } timestamp)
        {
            writer.WriteString("commitTimestamp", timestamp);
        }
        writer.WriteBoolean("isBackfill", metadata.IsBackfill);
        writer.WriteEndObject();
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
                "KafkaSinkOptions.SerializerOptions with a source-generated JsonSerializerContext covering the type.");
        }

        JsonSerializer.Serialize(writer, value, value.GetType(), serializerOptions ?? JsonSerializerOptions.Default);
    }
}
