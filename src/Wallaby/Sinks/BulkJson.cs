using System.Buffers;
using System.Text.Json;
using Wallaby.Abstractions;

namespace Wallaby.Sinks;

/// <summary>
/// The <c>_bulk</c> API dialect shared by Elasticsearch and OpenSearch (used by both sinks, and
/// available to custom sinks): NDJSON request bodies and per-item response classification. A body is
/// an action line (<c>index</c>/<c>delete</c> with <c>_index</c> and <c>_id</c>) followed by the
/// document line for upserts; document values are written by <see cref="SinkEnvelopeJson"/>.
/// </summary>
public static class BulkJson
{
    private static readonly byte[] NewLine = "\n"u8.ToArray();

    /// <summary>
    /// Write records <paramref name="offset"/>..<paramref name="offset"/>+<paramref name="count"/> as one
    /// bulk body. Each record's index is <see cref="SinkRecord.Destination"/>, falling back to
    /// <paramref name="defaultIndex"/>.
    /// </summary>
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
                SinkEnvelopeJson.WriteDocument(writer, record.Document!, record.DocumentId, serializerOptions);
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

    /// <summary>
    /// Classify a 2xx bulk response body: per-item failures are reported under <c>errors</c>/<c>items</c>.
    /// Deleting an already-absent document is success (deletes are idempotent under at-least-once delivery);
    /// throttling/server item failures are retryable (re-sending the whole chunk is safe; actions are
    /// idempotent by <c>_id</c>); other item rejections (mapping/parse) are permanent. A permanent item
    /// outweighs retryable ones. Null when every action applied; a missing or unparseable body is
    /// retryable. <paramref name="sinkDisplayName"/> names the destination system in failure messages.
    /// </summary>
    public static DeliveryResult? ClassifyItems(string? body, string sinkDisplayName)
    {
        if (string.IsNullOrEmpty(body))
        {
            return DeliveryResult.Retry($"{sinkDisplayName} returned an empty bulk response body.");
        }

        int retryable = 0, permanent = 0;
        string? firstPermanent = null;

        try
        {
            using var doc = JsonDocument.Parse(body);
            if (!doc.RootElement.TryGetProperty("errors", out var errors) || !errors.GetBoolean())
            {
                return null;
            }

            foreach (var wrapper in doc.RootElement.GetProperty("items").EnumerateArray())
            {
                // Each item is an object with a single property named after the action ("index"/"delete").
                foreach (var action in wrapper.EnumerateObject())
                {
                    var status = action.Value.GetProperty("status").GetInt32();
                    if (status < 300 || (status == 404 && action.Name == "delete"))
                    {
                        continue;
                    }

                    if (status is 408 or 429 or >= 500)
                    {
                        retryable++;
                    }
                    else
                    {
                        permanent++;
                        firstPermanent ??= DescribeItem(action.Value, status);
                    }
                }
            }
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException or FormatException)
        {
            return DeliveryResult.Retry($"{sinkDisplayName} returned an unrecognized bulk response: {ex.Message}", ex);
        }

        return permanent > 0
            ? DeliveryResult.Permanent($"{sinkDisplayName} rejected {permanent} bulk action(s); first: {firstPermanent}")
            : retryable > 0
                ? DeliveryResult.Retry($"{sinkDisplayName} reported {retryable} retryable bulk action failure(s).")
                : null;
    }

    private static string DescribeItem(JsonElement action, int status)
    {
        var id = action.TryGetProperty("_id", out var idElement) ? idElement.GetString() : null;
        string? error = null;
        if (action.TryGetProperty("error", out var errorElement) && errorElement.ValueKind == JsonValueKind.Object)
        {
            var type = errorElement.TryGetProperty("type", out var t) ? t.GetString() : null;
            var reason = errorElement.TryGetProperty("reason", out var r) ? r.GetString() : null;
            error = $"{type}: {reason}";
        }
        return $"_id '{id}' failed with {status} ({error ?? "no detail"})";
    }
}
