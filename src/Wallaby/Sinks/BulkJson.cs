using System.Buffers;
using System.Text.Json;
using Wallaby.Abstractions;

namespace Wallaby.Sinks;

/// <summary>
/// The <c>_bulk</c> API dialect shared by Elasticsearch and OpenSearch (and available to custom
/// sinks): NDJSON request bodies and per-item response classification. A body is
/// an action line (<c>index</c>/<c>delete</c> with <c>_index</c> and <c>_id</c>) followed by the
/// document line for upserts; document values are written by <see cref="SinkEnvelopeJson"/>.
/// </summary>
public static class BulkJson
{
    private static readonly byte[] NewLine = "\n"u8.ToArray();

    /// <summary>The <c>_delete_by_query</c> body matching every document in an index, used by purges.</summary>
    public static ReadOnlyMemory<byte> MatchAllQuery { get; } = """{"query":{"match_all":{}}}"""u8.ToArray();

    /// <summary>
    /// Write records <paramref name="offset"/>..<paramref name="offset"/>+<paramref name="count"/> as one
    /// bulk body into <paramref name="destination"/>. Each record's index is
    /// <see cref="SinkRecord.Destination"/>, falling back to <paramref name="defaultIndex"/> (see
    /// <see cref="SinkDestination"/>).
    /// </summary>
    public static void Write(
        IBufferWriter<byte> destination,
        string sinkName,
        IReadOnlyList<SinkRecord> records,
        int offset,
        int count,
        string? defaultIndex,
        JsonSerializerOptions? serializerOptions)
    {
        using var writer = new Utf8JsonWriter(destination);

        for (var i = offset; i < offset + count; i++)
        {
            var record = records[i];
            var index = SinkDestination.Resolve(record, defaultIndex, sinkName, "DefaultIndex");

            WriteAction(writer, destination, record, index);
            if (!record.IsDeletion)
            {
                SinkEnvelopeJson.WriteDocument(writer, record.Document!, record.DocumentId, serializerOptions);
                EndLine(writer, destination);
            }
        }
    }

    private static void WriteAction(Utf8JsonWriter writer, IBufferWriter<byte> destination, SinkRecord record, string index)
    {
        writer.WriteStartObject();
        writer.WriteStartObject(record.IsDeletion ? "delete" : "index");
        writer.WriteString("_index", index);
        writer.WriteString("_id", record.DocumentId);
        writer.WriteEndObject();
        writer.WriteEndObject();
        EndLine(writer, destination);
    }

    /// <summary>Commit the current JSON line, append the NDJSON newline, and reset for the next line.</summary>
    private static void EndLine(Utf8JsonWriter writer, IBufferWriter<byte> destination)
    {
        writer.Flush();
        destination.Write(NewLine);
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
    public static DeliveryResult? ClassifyItems(ReadOnlyMemory<byte> body, string sinkDisplayName)
    {
        if (body.IsEmpty)
        {
            return DeliveryResult.Retry($"{sinkDisplayName} returned an empty bulk response body.");
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            if (!doc.RootElement.TryGetProperty("errors", out var errors) || !errors.GetBoolean())
            {
                return null;
            }

            var items = ParseItems(doc.RootElement.GetProperty("items"));
            return ClassifyItems(items, sinkDisplayName);
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException or FormatException)
        {
            return DeliveryResult.Retry($"{sinkDisplayName} returned an unrecognized bulk response: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Classify already-parsed bulk response items with the same rules as
    /// <see cref="ClassifyItems(ReadOnlyMemory{byte}, string)"/>, for sinks whose client returns typed items.
    /// Null when every action applied.
    /// </summary>
    public static DeliveryResult? ClassifyItems(IEnumerable<BulkItemResult> items, string sinkDisplayName)
    {
        int retryable = 0, permanent = 0;
        string? firstPermanent = null;

        foreach (var item in items)
        {
            if (item.Status < 300 || (item.Status == 404 && item.Action == "delete"))
            {
                continue;
            }

            if (item.Status is 408 or 429 or >= 500)
            {
                retryable++;
            }
            else
            {
                permanent++;
                firstPermanent ??= $"_id '{item.Id}' failed with {item.Status} ({item.Error ?? "no detail"})";
            }
        }

        return permanent > 0
            ? DeliveryResult.Permanent($"{sinkDisplayName} rejected {permanent} bulk action(s); first: {firstPermanent}")
            : retryable > 0
                ? DeliveryResult.Retry($"{sinkDisplayName} reported {retryable} retryable bulk action failure(s).")
                : null;
    }

    /// <summary>
    /// Inspect a 2xx <c>_delete_by_query</c> response: null when every matched document was deleted,
    /// otherwise a description of the first entry in <c>failures</c> (with <c>conflicts=proceed</c>
    /// version conflicts are not failures, so an entry is a shard or document error). An unparseable
    /// body is reported as a failure too.
    /// </summary>
    public static string? DescribeDeleteByQueryFailure(ReadOnlyMemory<byte> body)
    {
        if (body.IsEmpty)
        {
            return "empty response body";
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            if (!doc.RootElement.TryGetProperty("failures", out var failures) || failures.ValueKind != JsonValueKind.Array)
            {
                return null;
            }

            foreach (var failure in failures.EnumerateArray())
            {
                return failure.GetRawText();
            }
            return null;
        }
        catch (JsonException ex)
        {
            return $"unrecognized response: {ex.Message}";
        }
    }

    private static IEnumerable<BulkItemResult> ParseItems(JsonElement items)
    {
        foreach (var wrapper in items.EnumerateArray())
        {
            // Each item is an object with a single property named after the action ("index"/"delete").
            foreach (var action in wrapper.EnumerateObject())
            {
                var status = action.Value.GetProperty("status").GetInt32();
                if (status < 300)
                {
                    // Applied items skip the name, id and error reads, so they allocate nothing.
                    yield return new BulkItemResult("", status, null, null);
                    continue;
                }

                var id = action.Value.TryGetProperty("_id", out var idElement) ? idElement.GetString() : null;
                var error = DescribeError(action.Value);
                yield return new BulkItemResult(action.Name, status, id, error);
            }
        }
    }

    private static string? DescribeError(JsonElement action)
    {
        if (!action.TryGetProperty("error", out var error) || error.ValueKind != JsonValueKind.Object)
        {
            return null;
        }
        var type = error.TryGetProperty("type", out var t) ? t.GetString() : null;
        var reason = error.TryGetProperty("reason", out var r) ? r.GetString() : null;
        return $"{type}: {reason}";
    }
}
