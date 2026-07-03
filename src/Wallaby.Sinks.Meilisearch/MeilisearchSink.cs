using Meilisearch;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Meilisearch;

/// <summary>
/// A destination that keeps Meilisearch indexes in sync with Postgres changes. Upserts are written with
/// <see cref="MeilisearchSinkOptions.PrimaryKey"/> set to the record's document id (so updates are
/// idempotent), and deletions remove by that same id. Records are routed to the index named by
/// <see cref="SinkRecord.Destination"/> (falling back to <see cref="MeilisearchSinkOptions.DefaultIndex"/>).
/// </summary>
public sealed class MeilisearchSink : ISink, ISinkInitializer
{
    private readonly MeilisearchSinkOptions _options;
    private readonly MeilisearchClient _client;

    // Per configured index, the attribute keys every document must carry (empty unless
    // ValidateConfiguredAttributes is on). Indexes not declared via ConfigureIndex are absent and unchecked.
    private readonly IReadOnlyDictionary<string, IReadOnlyCollection<string>> _requiredAttributes;

    /// <summary>
    /// Creates a sink that delivers to the Meilisearch instance described by <paramref name="options"/>.
    /// The underlying client (and its HTTP connection pool) is created once and reused for the
    /// lifetime of the sink.
    /// </summary>
    /// <param name="name">The sink's registration name; mappings route to it via <c>ToSink(name, ...)</c>.</param>
    /// <param name="options">Connection, index, and delivery-behaviour settings.</param>
    public MeilisearchSink(string name, MeilisearchSinkOptions options)
    {
        Name = name;
        _options = options;
        _client = new MeilisearchClient(options.Host, options.ApiKey);
        _requiredAttributes = BuildRequiredAttributes(options);
    }

    private static IReadOnlyDictionary<string, IReadOnlyCollection<string>> BuildRequiredAttributes(
        MeilisearchSinkOptions options)
    {
        var map = new Dictionary<string, IReadOnlyCollection<string>>(StringComparer.Ordinal);
        if (!options.ValidateConfiguredAttributes)
        {
            return map;
        }

        foreach (var config in options.Indexes)
        {
            if (config.Settings is null)
            {
                continue;
            }

            var required = new HashSet<string>(StringComparer.Ordinal);
            AddAttributes(required, config.Settings.SearchableAttributes);
            AddFilterableAttributes(required, config.Settings.FilterableAttributes);
            AddAttributes(required, config.Settings.SortableAttributes);

            // "*" is Meilisearch's "all attributes" wildcard, not a field name; the primary key is stamped on
            // every document by the sink, so neither needs to come from the transform.
            required.Remove("*");
            required.Remove(options.PrimaryKey);

            if (required.Count > 0)
            {
                map[config.Name] = required;
            }
        }

        return map;

        static void AddAttributes(HashSet<string> set, IEnumerable<string>? attributes)
        {
            if (attributes is null)
            {
                return;
            }

            foreach (var attribute in attributes)
            {
                set.Add(attribute);
            }
        }

        // Meilisearch 0.20 changed FilterableAttributes from strings to FilterableAttribute (a plain name, or a
        // { attributePatterns, features } object — the v1.14 form that opts filter features in/out). A document
        // is validated against every concrete field name: the legacy string form (surfaced by the SDK's implicit
        // conversion as Attribute) and any wildcard-free attribute pattern. Patterns containing '*' (e.g.
        // "user.*") match a family of fields, not one key, so a document can't be checked against them — skip.
        static void AddFilterableAttributes(HashSet<string> set, IEnumerable<FilterableAttribute>? attributes)
        {
            if (attributes is null)
            {
                return;
            }

            foreach (var attribute in attributes)
            {
                if (!string.IsNullOrEmpty(attribute.Attribute))
                {
                    set.Add(attribute.Attribute);
                }

                if (attribute.AttributePatterns is not null)
                {
                    foreach (var pattern in attribute.AttributePatterns)
                    {
                        if (!string.IsNullOrEmpty(pattern) && !pattern.Contains('*'))
                        {
                            set.Add(pattern);
                        }
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken ct)
    {
        foreach (var config in _options.Indexes)
        {
            var index = _client.Index(config.Name);

            if (!await IndexExistsAsync(config.Name, ct))
            {
                var created = await _client.CreateIndexAsync(config.Name, _options.PrimaryKey, ct);
                await WaitAsync(index, created, ct, force: true);
            }

            if (config.Settings is not null)
            {
                var updated = await index.UpdateSettingsAsync(config.Settings, ct);
                await WaitAsync(index, updated, ct, force: true);
            }
        }
    }

    private async Task<bool> IndexExistsAsync(string name, CancellationToken ct)
    {
        try
        {
            await _client.GetIndexAsync(name, ct);
            return true;
        }
        catch (MeilisearchApiError)
        {
            return false;
        }
    }

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
        catch (MeilisearchDocumentValidationException ex)
        {
            // A configured attribute is absent from the document — a configuration/transform bug. Retrying
            // would never succeed, so fail permanently (the dispatcher halts the pipeline).
            return DeliveryResult.Permanent(ex.Message, ex);
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

    private async Task WaitAsync(global::Meilisearch.Index index, TaskInfo info, CancellationToken ct, bool force = false)
    {
        // Index setup (force=true) always waits so the index is ready before streaming; delivery waits
        // only when WaitForCompletion is set.
        if (!force && !_options.WaitForCompletion)
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
                ValidateConfiguredAttributes(indexName, record.DocumentId, record.Document!);
                group.Upserts.Add(BuildUpsertDocument(record.Document!, id));
            }
        }

        return ordered;
    }

    /// <summary>
    /// When attribute validation is enabled, ensures the document carries a key for every attribute the index
    /// was configured with. Throws <see cref="MeilisearchDocumentValidationException"/> (a permanent failure)
    /// otherwise. A key with a null value counts as present — only an absent key is a problem.
    /// </summary>
    private void ValidateConfiguredAttributes(string indexName, string documentId,
        IReadOnlyDictionary<string, object?> document)
    {
        if (!_requiredAttributes.TryGetValue(indexName, out var required))
        {
            return;
        }

        List<string>? missing = null;
        foreach (var attribute in required)
        {
            if (!document.ContainsKey(attribute))
            {
                (missing ??= []).Add(attribute);
            }
        }

        if (missing is not null)
        {
            throw new MeilisearchDocumentValidationException(indexName, documentId, missing);
        }
    }

    private IReadOnlyDictionary<string, object?> BuildUpsertDocument(IReadOnlyDictionary<string, object?> document,
        string id)
    {
        // Documents are field bags. Copy defensively (so a transform-returned dictionary isn't mutated)
        // and stamp the primary key; the Meilisearch client serializes the dictionary as-is.
        var copy = new Dictionary<string, object?>(document.Count + 1, StringComparer.Ordinal);
        foreach (var kvp in document)
        {
            copy[kvp.Key] = kvp.Value;
        }

        copy[_options.PrimaryKey] = id;
        return copy;
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
        public List<IReadOnlyDictionary<string, object?>> Upserts { get; } = [];
        public List<string> Deletions { get; } = [];
    }
}
