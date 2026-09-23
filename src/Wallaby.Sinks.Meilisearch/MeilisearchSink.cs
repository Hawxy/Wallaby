using System.Buffers;
using System.Text;
using System.Text.Json;
using Meilisearch;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Meilisearch;

/// <summary>
/// A destination that keeps Meilisearch indexes in sync with Postgres changes. Upserts are written with
/// <see cref="MeilisearchSinkOptions.PrimaryKey"/> set to the record's document id (so updates are
/// idempotent), and deletions remove by that same id. Records are routed to the index named by
/// <see cref="SinkRecord.Destination"/> (falling back to <see cref="MeilisearchSinkOptions.DefaultIndex"/>).
/// Requests are sent through an <see cref="IHttpMessageHandlerFactory"/> named handler pipeline (see
/// <see cref="ClientNameFor"/>), so proxies, resilience handlers, and lifetimes are configured on the
/// named client.
/// </summary>
public sealed class MeilisearchSink : ISink, ISinkInitializer, ISinkPurger
{
    private static readonly SearchValues<string> PermanentErrorCodes = SearchValues.Create(
        [
            "invalid_api_key",
            "missing_authorization_header",
            "payload_too_large",
            "invalid_document_id",
            "missing_document_id",
            "invalid_document_fields",
            "invalid_document_geo_field",
            "invalid_index_uid",
            "invalid_index_primary_key",
            "index_primary_key_already_exists",
            "index_primary_key_multiple_candidates_found",
            "bad_request",
        ],
        StringComparison.Ordinal);

    private readonly MeilisearchSinkOptions _options;
    private readonly Func<HttpMessageHandler> _transport;
    private readonly Uri _baseAddress;

    // Per configured index, the attribute keys every document must carry (empty unless
    // ValidateConfiguredAttributes is on). Indexes not declared via ConfigureIndex are absent and unchecked.
    private readonly IReadOnlyDictionary<string, IReadOnlyCollection<string>> _requiredAttributes;

    /// <summary>
    /// Creates a sink that delivers to the Meilisearch instance described by <paramref name="options"/>.
    /// The handler pipeline is drawn from <paramref name="factory"/> per delivery, so named-client
    /// configuration (handlers, lifetimes) applies without caching a connection pool on this
    /// long-lived sink.
    /// </summary>
    /// <param name="name">The sink's registration name (used for routing, telemetry, and test replacement).</param>
    /// <param name="options">Connection, index, and delivery-behaviour settings.</param>
    /// <param name="factory">Factory providing the named handler pipeline.</param>
    public MeilisearchSink(string name, MeilisearchSinkOptions options, IHttpMessageHandlerFactory factory)
        : this(name, options, TransportFor(factory, options.HttpClientName ?? ClientNameFor(name)))
    {
    }

    internal MeilisearchSink(string name, MeilisearchSinkOptions options, Func<HttpMessageHandler> transport)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);
        MeilisearchBuilderExtensions.Validate(options);
        Name = name;
        _options = options;
        _transport = transport;
        // The base address must end with '/' for the client's relative request URIs to resolve under it.
        _baseAddress = new Uri(options.Endpoint.EndsWith('/') ? options.Endpoint : options.Endpoint + "/");
        _requiredAttributes = BuildRequiredAttributes(options);
    }

    private static Func<HttpMessageHandler> TransportFor(IHttpMessageHandlerFactory factory, string clientName)
        => () => factory.CreateHandler(clientName);

    /// <summary>
    /// The default <see cref="IHttpMessageHandlerFactory"/> client name for a sink,
    /// <c>wallaby.sinks.meilisearch.&lt;name&gt;</c>. Configure the pipeline on it:
    /// <c>services.AddHttpClient(MeilisearchSink.ClientNameFor("meili")).ConfigurePrimaryHttpMessageHandler(...)</c>.
    /// </summary>
    public static string ClientNameFor(string sinkName) => $"wallaby.sinks.meilisearch.{sinkName}";

    // A client per operation: the factory owns the (pooled) handler's lifetime, so neither the client nor
    // the MeilisearchMessageHandler wrapper (which converts error responses into MeilisearchApiError —
    // the SDK relies on it) may dispose it.
    private MeilisearchClient CreateClient()
        => new(
            new HttpClient(new MeilisearchMessageHandler(_transport()), disposeHandler: false)
            {
                BaseAddress = _baseAddress,
            },
            _options.ApiKey);

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
        var client = CreateClient();
        foreach (var config in _options.Indexes)
        {
            var index = client.Index(config.Name);

            if (!await IndexExistsAsync(client, config.Name, ct))
            {
                var created = await client.CreateIndexAsync(config.Name, _options.PrimaryKey, ct);
                await WaitAsync(index, created, ct);
            }

            if (config.Settings is not null)
            {
                var updated = await index.UpdateSettingsAsync(config.Settings, ct);
                await WaitAsync(index, updated, ct);
            }
        }
    }

    /// <inheritdoc />
    public async Task PurgeAsync(SinkPurgeRequest request, CancellationToken ct)
    {
        var indexName = SinkDestination.Resolve(request, _options.DefaultIndex, Name, nameof(_options.DefaultIndex));

        var index = CreateClient().Index(indexName);
        try
        {
            var info = await index.DeleteAllDocumentsAsync(ct);
            await WaitAsync(index, info, ct);
        }
        catch (Exception ex) when (IsIndexNotFound(ex))
        {
            // Nothing to purge; InitializeAsync creates configured indexes before the scheduler runs.
        }
    }

    // The absent index surfaces from WaitAsync (as a failed task) when the request enqueues, or
    // synchronously (as an API error) when the request itself 404s.
    private static bool IsIndexNotFound(Exception ex)
        => ex is MeilisearchTaskFailedException { Code: "index_not_found" }
            or MeilisearchApiError { Code: "index_not_found" };

    private static async Task<bool> IndexExistsAsync(MeilisearchClient client, string name, CancellationToken ct)
    {
        try
        {
            await client.GetIndexAsync(name, ct);
            return true;
        }
        catch (MeilisearchApiError ex) when (ex.Code == "index_not_found")
        {
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        try
        {
            var client = CreateClient();
            var groups = GroupByIndex(batch.Records);

            // Index-level operations are independent; fan out across indexes in parallel.
            // Within each index we still preserve the upsert-before-delete order.
            var tasks = new Task[groups.Count];
            for (var i = 0; i < groups.Count; i++)
            {
                tasks[i] = DispatchGroupAsync(client, groups[i], ct);
            }
            await Task.WhenAll(tasks);

            return DeliveryResult.Success;
        }
        catch (Exception ex) when (ex is MeilisearchDocumentValidationException or MeilisearchDocumentIdException)
        {
            // A configured attribute is absent, or the id can't be stored: a configuration/transform bug.
            // Retrying would never succeed, so fail permanently (the dispatcher halts the pipeline).
            return DeliveryResult.Permanent(ex.Message, ex);
        }
        catch (MeilisearchTaskFailedException ex)
        {
            return ClassifyByCode(ex.Code, ex.Message, ex);
        }
        catch (MeilisearchApiError ex)
        {
            return ClassifyByCode(ex.Code, $"Meilisearch request failed ({ex.Code ?? "no code"}): {ex.Message}", ex);
        }
        catch (MeilisearchSerializationException ex)
        {
            // A document value the JSON writer can't encode is a transform/configuration bug; retrying
            // would never succeed.
            return DeliveryResult.Permanent(ex.Message, ex.InnerException);
        }
        catch (Exception ex) when (IsTransportFailure(ex))
        {
            return DeliveryResult.Retry($"Meilisearch delivery failed: {ex.Message}", ex);
        }
    }

    // Connection, socket and timeout failures, including the SDK's own wrappers and a cancelled
    // per-request timeout (the dispatcher turns a retryable result under a cancelled token into
    // cancellation). A JsonException here comes from the SDK parsing a response that is not
    // Meilisearch JSON (a proxy or load-balancer error page), so it is transport too.
    private static bool IsTransportFailure(Exception ex)
        => ex is HttpRequestException or IOException or TimeoutException or OperationCanceledException
            or MeilisearchCommunicationError or MeilisearchTimeoutError or JsonException;

    private static DeliveryResult ClassifyByCode(string? code, string description, Exception exception)
        => code is not null && PermanentErrorCodes.Contains(code)
            ? DeliveryResult.Permanent(description, exception)
            : DeliveryResult.Retry(description, exception);

    private async Task DispatchGroupAsync(MeilisearchClient client, IndexGroup group, CancellationToken ct)
    {
        var index = client.Index(group.Index);

        // Chunks keep each request payload well under Meilisearch's body limit; upserts complete before
        // deletions so a delete always wins over an earlier upsert of the same document in the batch.
        for (var offset = 0; offset < group.Upserts.Count; offset += _options.MaxRecordsPerRequest)
        {
            var count = Math.Min(_options.MaxRecordsPerRequest, group.Upserts.Count - offset);
            var info = await index.AddDocumentsJsonAsync(WriteDocumentsJson(group.Upserts, offset, count), _options.PrimaryKey, ct);
            await WaitAsync(index, info, ct);
        }

        for (var offset = 0; offset < group.Deletions.Count; offset += _options.MaxRecordsPerRequest)
        {
            var count = Math.Min(_options.MaxRecordsPerRequest, group.Deletions.Count - offset);
            try
            {
                var info = await index.DeleteDocumentsAsync(group.Deletions.GetRange(offset, count), ct);
                await WaitAsync(index, info, ct);
            }
            // Deletes don't auto-create the index (upserts do), so a delete-only batch to an index that
            // was never written has nothing to remove. Retrying can never create it; treating this as a
            // failure would loop the whole batch forever.
            catch (Exception ex) when (IsIndexNotFound(ex))
            {
                break;
            }
        }
    }

    // Documents are written with the shared reflection-free writer, so values encode exactly as the
    // other sinks encode them and SerializerOptions covers the rest.
    private string WriteDocumentsJson(List<IReadOnlyDictionary<string, object?>> documents, int offset, int count)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer);
        try
        {
            writer.WriteStartArray();
            for (var i = offset; i < offset + count; i++)
            {
                var document = documents[i];
                SinkEnvelopeJson.WriteDocument(writer, document, (string)document[_options.PrimaryKey]!, _options.SerializerOptions);
            }
            writer.WriteEndArray();
            writer.Flush();
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            throw new MeilisearchSerializationException(ex);
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private async Task WaitAsync(global::Meilisearch.Index index, TaskInfo info, CancellationToken ct)
    {
        // Every task is awaited to completion, so a batch is only reported delivered (and the LSN acked)
        // once Meilisearch has actually applied it.
        var result = await index.WaitForTaskAsync(
            info.TaskUid, _options.WaitTimeout.TotalMilliseconds, (int)_options.WaitInterval.TotalMilliseconds, ct);
        if (result.Status is TaskInfoStatus.Failed or TaskInfoStatus.Canceled)
        {
            string? code = null;
            var detail = "(no detail)";
            if (result.Error is not null)
            {
                result.Error.TryGetValue("code", out code);
                detail = string.Join("; ", result.Error.Select(kv => $"{kv.Key}={kv.Value}"));
            }
            throw new MeilisearchTaskFailedException(info.TaskUid, result.Status, code, detail);
        }
    }

    private List<IndexGroup> GroupByIndex(IReadOnlyList<SinkRecord> records)
    {
        var groups = new Dictionary<string, IndexGroup>();
        var ordered = new List<IndexGroup>();

        foreach (var record in records)
        {
            var indexName = SinkDestination.Resolve(record, _options.DefaultIndex, Name, nameof(_options.DefaultIndex));

            if (!groups.TryGetValue(indexName, out var group))
            {
                group = new IndexGroup(indexName);
                groups[indexName] = group;
                ordered.Add(group);
            }

            var id = MeilisearchDocumentIds.Encode(record.DocumentId);
            if (record.IsDeletion)
            {
                group.Deletions.Add(id);
            }
            else
            {
                ValidateConfiguredAttributes(indexName, record.DocumentId, record.Document!);
                group.Upserts.Add(BuildUpsertDocument(indexName, record.DocumentId, record.Document!, id));
            }
        }

        return ordered;
    }

    /// <summary>
    /// When attribute validation is enabled, ensures the document carries a key for every attribute the index
    /// was configured with. Throws <see cref="MeilisearchDocumentValidationException"/> (a permanent failure)
    /// otherwise. A key with a null value counts as present — only an absent key is a problem. A dotted
    /// attribute matches a literal key first, then resolves segment-by-segment the way Meilisearch does:
    /// through nested dictionaries and through the elements of an array. Validation only inspects
    /// dictionary-shaped values; a segment landing on anything else (a POCO, an anonymous type, a scalar)
    /// passes, so only a dictionary provably lacking a key is ever reported.
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
            if (!AttributeSatisfied(document, attribute))
            {
                (missing ??= []).Add(attribute);
            }
        }

        if (missing is not null)
        {
            throw new MeilisearchDocumentValidationException(indexName, documentId, missing);
        }
    }

    private static bool AttributeSatisfied(IReadOnlyDictionary<string, object?> document, string attribute)
    {
        // A literal key always satisfies (including keys that happen to contain a dot).
        if (document.ContainsKey(attribute))
        {
            return true;
        }
        return attribute.Contains('.') && SegmentsSatisfied(document, attribute.Split('.'), 0);
    }

    private static bool SegmentsSatisfied(object? value, string[] segments, int index)
    {
        if (index == segments.Length)
        {
            return true; // every segment resolved; a null leaf counts as present
        }

        switch (value)
        {
            case IReadOnlyDictionary<string, object?> nested:
                return nested.TryGetValue(segments[index], out var next)
                    && SegmentsSatisfied(next, segments, index + 1);
            case string:
                return true; // scalar dead-end; not provably missing
            case System.Collections.IEnumerable items:
            {
                // Meilisearch resolves a dotted path through arrays of objects: satisfied when any
                // element satisfies the remainder. An empty array is data, not a transform bug.
                var any = false;
                foreach (var item in items)
                {
                    any = true;
                    if (SegmentsSatisfied(item, segments, index))
                    {
                        return true;
                    }
                }
                return !any;
            }
            default:
                return true; // unknown shape (POCO/anonymous/scalar): not ours to judge
        }
    }

    private IReadOnlyDictionary<string, object?> BuildUpsertDocument(string indexName, string documentId,
        IReadOnlyDictionary<string, object?> document, string id)
    {
        // A primary-key field the transform emitted must already hold the id: stamping over a different value
        // would make readers mistake the id for their own key.
        if (document.TryGetValue(_options.PrimaryKey, out var emitted) && emitted is not null
            && new DocumentKey(emitted).ToString() != id)
        {
            throw new MeilisearchDocumentIdException(documentId,
                $"Document '{documentId}' routed to Meilisearch index '{indexName}' has a '{_options.PrimaryKey}' " +
                $"field ('{emitted}') that differs from its Meilisearch id '{id}'. The sink stores the document id " +
                $"in that field; drop it from the transform, or set {nameof(MeilisearchSinkOptions.PrimaryKey)} " +
                "to another field name.");
        }

        // Documents are field bags. Copy defensively (so a transform-returned dictionary isn't mutated)
        // and stamp the primary key.
        var copy = new Dictionary<string, object?>(document.Count + 1, StringComparer.Ordinal);
        foreach (var kvp in document)
        {
            copy[kvp.Key] = kvp.Value;
        }

        copy[_options.PrimaryKey] = id;
        return copy;
    }

    private sealed class IndexGroup(string index)
    {
        public string Index { get; } = index;
        public List<IReadOnlyDictionary<string, object?>> Upserts { get; } = [];
        public List<string> Deletions { get; } = [];
    }
}
