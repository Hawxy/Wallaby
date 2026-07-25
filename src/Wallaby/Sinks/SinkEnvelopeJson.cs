using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;
using Wallaby.Abstractions;

namespace Wallaby.Sinks;

/// <summary>
/// The record-level JSON shared by sink envelope writers (used by the HTTP and Kafka sinks, and
/// available to custom sinks): metadata, document values, and the idempotency key. Common scalar
/// values are written directly with <see cref="Utf8JsonWriter"/> (reflection-free); other value types
/// go through the consumer's <see cref="JsonSerializerOptions"/>, falling back to reflection-based
/// serialization only where the host supports it. Each sink keeps its own envelope shape around
/// these pieces.
/// </summary>
public static class SinkEnvelopeJson
{
    /// <summary>
    /// An opaque per-record deduplication key. Unique per delivered change. Backfill keys embed a per-run
    /// token, so separate runs over the same row (e.g. a version-triggered re-backfill) produce distinct
    /// keys; within one run the key is stable across its chunks. A run interrupted by a crash resumes
    /// under a fresh token, so its rows can re-deliver with new keys (backfill is upsert-only, so replays
    /// are harmless to idempotent consumers).
    /// </summary>
    public static string IdempotencyKey(SinkRecord record)
    {
        var metadata = record.Metadata;
        var scope = record.Destination ?? metadata.QualifiedTableName;
        return metadata.IsBackfill
            ? $"backfill:{metadata.BackfillRunId ?? "0"}:{scope}:{record.DocumentId}"
            : $"{metadata.CommitLsn}:{metadata.CommitIdx}:{scope}:{record.DocumentId}";
    }

    /// <summary>
    /// Writes a <c>"metadata"</c> object property carrying the record's source provenance:
    /// schema, table, action, commit position, commit timestamp when known, the backfill flag, and the
    /// backfill run id when present.
    /// </summary>
    public static void WriteMetadata(Utf8JsonWriter writer, ChangeMetadata metadata)
    {
        writer.WriteStartObject("metadata");
        writer.WriteString("schema", metadata.TableSchema);
        writer.WriteString("table", metadata.TableName);
        writer.WriteString("action", metadata.Action switch
        {
            ChangeAction.Insert => "insert",
            ChangeAction.Update => "update",
            ChangeAction.Delete => "delete",
            ChangeAction.Read => "read",
            // The action strings are a wire contract.
            _ => throw new UnreachableException($"Unmapped ChangeAction '{metadata.Action}'."),
        });
        // As a string: the ulong LSN can exceed the safe-integer range of JavaScript consumers.
        writer.WriteString("commitLsn", metadata.CommitLsn.ToString(CultureInfo.InvariantCulture));
        writer.WriteNumber("commitIdx", metadata.CommitIdx);
        if (metadata.CommitTimestamp is { } timestamp)
        {
            writer.WriteString("commitTimestamp", timestamp);
        }
        writer.WriteBoolean("isBackfill", metadata.IsBackfill);
        if (metadata.BackfillRunId is { } runId)
        {
            writer.WriteString("backfillRunId", runId);
        }
        writer.WriteEndObject();
    }

    /// <summary>
    /// Writes the document's field bag as a JSON object at the writer's current position.
    /// <paramref name="serializerOptionsName"/> is the sink's serializer-options setting, named in the no-fallback error.
    /// </summary>
    public static void WriteDocument(Utf8JsonWriter writer, IReadOnlyDictionary<string, object?> document,
        string documentId, JsonSerializerOptions? serializerOptions, string serializerOptionsName)
    {
        writer.WriteStartObject();
        foreach (var field in document)
        {
            writer.WritePropertyName(field.Key);
            WriteValue(writer, field.Value, field.Key, documentId, serializerOptions, serializerOptionsName);
        }
        writer.WriteEndObject();
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value, string key, string documentId,
        JsonSerializerOptions? serializerOptions, string serializerOptionsName)
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
                WriteDocument(writer, nested, documentId, serializerOptions, serializerOptionsName);
                break;
            case IEnumerable sequence:
                writer.WriteStartArray();
                foreach (var item in sequence)
                {
                    WriteValue(writer, item, key, documentId, serializerOptions, serializerOptionsName);
                }
                writer.WriteEndArray();
                break;
            default:
                WriteFallback(writer, value, key, documentId, serializerOptions, serializerOptionsName);
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
        JsonSerializerOptions? serializerOptions, string serializerOptionsName)
    {
        if (serializerOptions is null && !JsonSerializer.IsReflectionEnabledByDefault)
        {
            throw new NotSupportedException(
                $"Document '{documentId}' field '{key}' has type '{value.GetType()}', which has no built-in JSON " +
                "encoding, and reflection-based serialization is disabled (trimmed/NativeAOT host). Set " +
                $"{serializerOptionsName} with a source-generated JsonSerializerContext covering the type.");
        }

        JsonSerializer.Serialize(writer, value, value.GetType(), serializerOptions ?? JsonSerializerOptions.Default);
    }
}
