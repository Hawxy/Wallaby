using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Elastic.Ingest.Elasticsearch.Bulk;
using Elastic.Ingest.Elasticsearch.Serialization;
using Elastic.Transport;
using Wallaby.Abstractions;
using Wallaby.Sinks;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Wallaby.Sinks.Elasticsearch;

/// <summary>
/// A destination that keeps Elasticsearch indices in sync with Postgres changes via the <c>_bulk</c> API.
/// Upserts are indexed with <c>_id</c> set to the record's document id (so updates are idempotent), and
/// deletions remove by that same id. Records are routed to the index named by
/// <see cref="SinkRecord.Destination"/> (falling back to <see cref="ElasticsearchSinkOptions.DefaultIndex"/>);
/// indices are not created or configured by the sink: they auto-create on first write unless pre-created
/// with explicit settings/mappings. A purge empties an index with <c>_delete_by_query</c>.
/// </summary>
public sealed class ElasticsearchSink : ISink, ISinkPurger, IDisposable
{
    private readonly ElasticsearchSinkOptions _options;
    private readonly ITransportConfiguration _settings;
    private readonly ITransport _transport;
    private readonly BulkSender<SinkRecord, BulkDocumentBody> _bulkSender;

    /// <summary>
    /// Creates a sink that delivers to the Elasticsearch cluster described by <paramref name="options"/>.
    /// The underlying transport (and its connection pool) is created once and reused for the lifetime of
    /// the sink.
    /// </summary>
    /// <param name="name">The sink's registration name (used for routing, telemetry, and test replacement).</param>
    /// <param name="options">Connection, routing, and delivery-behaviour settings.</param>
    public ElasticsearchSink(string name, ElasticsearchSinkOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);
        ElasticsearchBuilderExtensions.Validate(options);
        Name = name;
        _options = options;
        var endpoint = new Uri(options.Endpoint, UriKind.Absolute);
        _settings = options.ConfigureConnection is not null
            ? options.ConfigureConnection(endpoint)
            : BuildSettings(endpoint, options);
        _transport = new DistributedTransport(_settings);

        // A custom JsonConverter<BulkDocumentBody> is the bridge between BulkSender's static
        // JsonTypeInfo<TBody> contract and the sink's dynamic per-record field bag: the converter just
        // delegates to the same reflection-free SinkEnvelopeJson.WriteDocument the sink always used.
        // ApplyLibrarySerializerDefaults is disabled because its re-resolution path
        // (copy.GetTypeInfo(typeof(TBody))) assumes a resolver-backed JsonSerializerOptions, which this
        // converter-only JsonTypeInfo does not have.
        // A dedicated, empty-resolver options instance, independent of options.SerializerOptions: STJ requires
        // *some* non-null TypeInfoResolver before a JsonTypeInfo can be used, even when (as here) a converter
        // is supplied directly and the resolver itself is never consulted. JsonTypeInfoResolver.Combine()
        // with no arguments satisfies that without pulling in reflection or touching the caller's own options
        // object (which BulkDocumentBodyConverter still receives separately, for SinkEnvelopeJson's fallback).
        var bodyTypeInfo = JsonMetadataServices.CreateValueInfo<BulkDocumentBody>(
            new JsonSerializerOptions { TypeInfoResolver = JsonTypeInfoResolver.Combine() },
            new BulkDocumentBodyConverter(options.SerializerOptions));

        _bulkSender = new BulkSender<SinkRecord, BulkDocumentBody>(new BulkSenderOptions<SinkRecord, BulkDocumentBody>
        {
            Transport = _transport,
            Action = ResolveAction,
            Body = r => new BulkDocumentBody(r.DocumentId, r.Document!),
            BodyTypeInfo = bodyTypeInfo,
            Retry = BulkRetryPolicy.None, // Wallaby's SinkDispatcher owns retry/backoff; one request per call.
            ApplyLibrarySerializerDefaults = false,
            Refresh = options.Refresh ? BulkRefresh.WaitFor : null,
            RequestTimeout = options.Timeout,
        });
    }

    private static ITransportConfiguration BuildSettings(Uri endpoint, ElasticsearchSinkOptions options)
    {
        var settings = new TransportConfigurationDescriptor(endpoint);
        if (options.ApiKey is not null)
        {
            settings.Authentication(new ApiKey(options.ApiKey));
        }
        else if (options.Username is not null)
        {
            settings.Authentication(new BasicAuthentication(options.Username, options.Password ?? ""));
        }
        return settings;
    }

    private BulkAction ResolveAction(SinkRecord record)
    {
        var index = SinkDestination.Resolve(record, _options.DefaultIndex, Name, nameof(_options.DefaultIndex));
        return record.IsDeletion ? BulkAction.Delete(record.DocumentId, index) : BulkAction.Index(record.DocumentId, index);
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        var records = batch.Records;

        // Chunks are sent sequentially so commit order is preserved across requests.
        for (var offset = 0; offset < records.Count; offset += _options.MaxRecordsPerRequest)
        {
            var count = Math.Min(_options.MaxRecordsPerRequest, records.Count - offset);

            BulkResponse response;
            try
            {
                // BulkSender.SendAsync serializes synchronously, so a record whose document value the
                // body converter can't encode (or an unresolvable destination) throws from this call,
                // exactly as BulkJson.Write used to.
                response = await _bulkSender.SendAsync(Slice(records, offset, count), ct);
            }
            catch (TransportException ex)
            {
                return DeliveryResult.Retry($"Elasticsearch bulk request failed: {ex.Message}", ex);
            }
            catch (Exception ex) when (ex is not WallabyConfigurationException and not OperationCanceledException)
            {
                return DeliveryResult.Permanent($"Elasticsearch bulk serialization failed: {ex.Message}", ex);
            }

            var failure = ClassifyResponse(records, offset, count, response);
            if (failure is not null)
            {
                return failure;
            }
        }

        return DeliveryResult.Success;
    }

    private static IEnumerable<SinkRecord> Slice(IReadOnlyList<SinkRecord> records, int offset, int count)
    {
        for (var i = offset; i < offset + count; i++)
        {
            yield return records[i];
        }
    }

    /// <summary>Classify one chunk's bulk response; null on success, otherwise the classified failure.</summary>
    private static DeliveryResult? ClassifyResponse(IReadOnlyList<SinkRecord> records, int offset, int count, BulkResponse response)
    {
        var status = response.ApiCallDetails.HttpStatusCode;
        if (status is null)
        {
            // No HTTP status: DNS/socket failure or the per-request timeout.
            return DeliveryResult.Retry(
                $"Elasticsearch bulk request failed: {response.ApiCallDetails.OriginalException?.Message ?? "no response"}",
                response.ApiCallDetails.OriginalException);
        }

        if (status is 408 or 429 or >= 500)
        {
            return DeliveryResult.Retry($"Elasticsearch bulk request received {status}.");
        }

        if (status is < 200 or >= 300)
        {
            return DeliveryResult.Permanent($"Elasticsearch bulk request was rejected with {status}.");
        }

        if (response.ApiCallDetails.OriginalException is not null)
        {
            // A transport failure can surface with a default status; never treat it as an applied bulk.
            return DeliveryResult.Retry(
                $"Elasticsearch bulk request failed: {response.ApiCallDetails.OriginalException.Message}",
                response.ApiCallDetails.OriginalException);
        }

        return ClassifyItems(records, offset, count, response);
    }

    /// <summary>
    /// Classify a 2xx bulk response: per-item failures are reported under <see cref="BulkResponse.Errors"/>/
    /// <see cref="BulkResponse.Items"/>, which line up positionally with the sent records (guaranteed by
    /// <see cref="BulkSender{TItem,TBody}"/>, even if a retry policy were enabled). Deleting an already-absent
    /// document is success (deletes are idempotent under at-least-once delivery); throttling/server item
    /// failures are retryable (re-sending the whole chunk is safe; actions are idempotent by <c>_id</c>); other
    /// item rejections (mapping/parse) are permanent. A permanent item outweighs retryable ones. Null when
    /// every action applied.
    /// </summary>
    private static DeliveryResult? ClassifyItems(IReadOnlyList<SinkRecord> records, int offset, int count, BulkResponse response)
    {
        if (response.Errors is not true || response.Items is null)
        {
            return null;
        }

        int retryable = 0, permanent = 0;
        string? firstPermanent = null;
        var i = 0;
        foreach (var item in response.Items)
        {
            var status = item.Status;
            var isDelete = item.Action == "delete";
            if (status < 300 || (status == 404 && isDelete))
            {
                i++;
                continue;
            }

            if (status is 408 or 429 or >= 500)
            {
                retryable++;
            }
            else
            {
                permanent++;
                if (firstPermanent is null)
                {
                    var id = i < count ? records[offset + i].DocumentId : "?";
                    firstPermanent = $"_id '{id}' failed with {status} ({item.Error?.ToString() ?? "no detail"})";
                }
            }
            i++;
        }

        return permanent > 0
            ? DeliveryResult.Permanent($"Elasticsearch rejected {permanent} bulk action(s); first: {firstPermanent}")
            : retryable > 0
                ? DeliveryResult.Retry($"Elasticsearch reported {retryable} retryable bulk action failure(s).")
                : null;
    }

    /// <inheritdoc />
    public async Task PurgeAsync(SinkPurgeRequest request, CancellationToken ct)
    {
        var index = SinkDestination.Resolve(request, _options.DefaultIndex, Name, nameof(_options.DefaultIndex));
        var path = $"/{Uri.EscapeDataString(index)}/_delete_by_query?conflicts=proceed&refresh=true";

        var response = await _transport.RequestAsync<BytesResponse>(
            new EndpointPath(HttpMethod.POST, path), PostData.ReadOnlyMemory(BulkJson.MatchAllQuery), null, RequestConfig(), ct);

        var status = response.ApiCallDetails.HttpStatusCode;
        if (status == 404)
        {
            return; // The index was never written: nothing to purge.
        }
        if (status is null or < 200 or >= 300 || response.ApiCallDetails.OriginalException is not null)
        {
            throw new InvalidOperationException(
                $"Elasticsearch purge of index '{index}' failed: " +
                (response.ApiCallDetails.OriginalException?.Message ?? $"status {status}"),
                response.ApiCallDetails.OriginalException);
        }
        if (BulkJson.DescribeDeleteByQueryFailure(response.Body) is { } failure)
        {
            throw new InvalidOperationException($"Elasticsearch purge of index '{index}' failed: {failure}");
        }
    }

    private RequestConfiguration RequestConfig() => new() { RequestTimeout = _options.Timeout };

    /// <inheritdoc />
    public void Dispose() => _settings.Dispose();
}
