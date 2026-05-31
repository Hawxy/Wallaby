using System.Text.Json;
using System.Text.Json.Nodes;
using Meilisearch;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Meilisearch;

/// <summary>
/// A destination that keeps Meilisearch indexes in sync with Postgres changes. Upserts are written with
/// <see cref="MeilisearchSinkOptions.PrimaryKey"/> set to the record's document id (so updates are
/// idempotent), and deletions remove by that same id. Records are routed to the index named by
/// <see cref="SinkRecord.Destination"/> (falling back to <see cref="MeilisearchSinkOptions.DefaultIndex"/>).
/// </summary>
public sealed class MeilisearchSink : ISink
{
    private readonly MeilisearchSinkOptions _options;
    private readonly MeilisearchClient _client;

    public MeilisearchSink(string name, MeilisearchSinkOptions options)
    {
        Name = name;
        _options = options;
        _client = new MeilisearchClient(options.Host, options.ApiKey);
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        try
        {
            var groups = GroupByIndex(batch.Records);
            if (groups.Count <= 1)
            {
                foreach (var group in groups)
                {
                    await DispatchGroupAsync(group, ct);
                }
            }
            else
            {
                // Index-level operations are independent; fan out across indexes in parallel.
                // Within each index we still preserve the upsert-before-delete order.
                var tasks = new Task[groups.Count];
                for (var i = 0; i < groups.Count; i++)
                {
                    tasks[i] = DispatchGroupAsync(groups[i], ct);
                }
                await Task.WhenAll(tasks);
            }

            return DeliveryResult.Success;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Network/HTTP/Meili task failures are treated as retryable; the dispatcher backs off.
            return DeliveryResult.Retry($"Meilisearch delivery failed: {ex.Message}", ex);
        }
    }

    private async Task DispatchGroupAsync(IndexGroup group, CancellationToken ct)
    {
        var index = _client.Index(group.Index);

        if (group.Upserts.Count > 0)
        {
            var info = await index.AddDocumentsAsync(group.Upserts, _options.PrimaryKey, ct);
            await WaitAsync(index, info, ct);
        }

        if (group.Deletions.Count > 0)
        {
            var info = await index.DeleteDocumentsAsync(group.Deletions, ct);
            await WaitAsync(index, info, ct);
        }
    }

    private async Task WaitAsync(global::Meilisearch.Index index, TaskInfo info, CancellationToken ct)
    {
        if (!_options.WaitForCompletion)
        {
            return;
        }

        var result = await index.WaitForTaskAsync(info.TaskUid, _options.WaitTimeoutMs, _options.WaitIntervalMs, ct);
        if (result.Status is TaskInfoStatus.Failed or TaskInfoStatus.Canceled)
        {
            var error = result.Error is null ? "(no detail)" : string.Join("; ", result.Error.Select(kv => $"{kv.Key}={kv.Value}"));
            throw new InvalidOperationException($"Meilisearch task {info.TaskUid} finished with status {result.Status}: {error}");
        }
    }

    private List<IndexGroup> GroupByIndex(IReadOnlyList<SinkRecord> records)
    {
        var groups = new Dictionary<string, IndexGroup>();
        var ordered = new List<IndexGroup>();

        foreach (var record in records)
        {
            var indexName = record.Destination ?? _options.DefaultIndex
                ?? throw new InvalidOperationException(
                    $"Record {record.DocumentId} has no destination and no DefaultIndex is configured for sink '{Name}'.");

            if (!groups.TryGetValue(indexName, out var group))
            {
                group = new IndexGroup(indexName);
                groups[indexName] = group;
                ordered.Add(group);
            }

            var id = SanitizeId(record.DocumentId);
            if (record.IsDeletion)
            {
                group.Deletions.Add(id);
            }
            else
            {
                group.Upserts.Add(BuildUpsertDocument(record.Document!, id));
            }
        }

        return ordered;
    }

    private object BuildUpsertDocument(object document, string id)
    {
        // Fast path: transforms in this codebase typically return Dictionary<string, object?>.
        // Stamp the primary key in place of a SerializeToNode round-trip.
        if (document is IDictionary<string, object?> dict)
        {
            // Defensive copy so transform-returned dictionaries aren't mutated by us.
            var copy = new Dictionary<string, object?>(dict.Count + 1, StringComparer.Ordinal);
            foreach (var kvp in dict)
            {
                copy[kvp.Key] = kvp.Value;
            }
            copy[_options.PrimaryKey] = id;
            return copy;
        }

        var node = JsonSerializer.SerializeToNode(document, document.GetType()) as JsonObject
                   ?? new JsonObject();
        node[_options.PrimaryKey] = JsonValue.Create(id);
        return node;
    }

    /// <summary>Meilisearch document ids allow only [a-zA-Z0-9-_]; replace anything else (e.g. composite-key separators).</summary>
    private static string SanitizeId(string id)
    {
        Span<char> buffer = id.Length <= 512 ? stackalloc char[id.Length] : new char[id.Length];
        for (var i = 0; i < id.Length; i++)
        {
            var ch = id[i];
            buffer[i] = ch is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '-' or '_' ? ch : '_';
        }
        var result = new string(buffer);
        return result.Length <= 511 ? result : result[..511];
    }

    private sealed class IndexGroup(string index)
    {
        public string Index { get; } = index;
        public List<object> Upserts { get; } = [];
        public List<string> Deletions { get; } = [];
    }
}
