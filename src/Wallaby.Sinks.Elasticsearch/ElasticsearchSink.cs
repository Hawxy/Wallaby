using Elastic.Ingest.Elasticsearch.Bulk;
using Elastic.Ingest.Elasticsearch.Serialization;
using Elastic.Transport;
using Wallaby.Abstractions;
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
    private readonly BulkSender<SinkRecord, SinkRecord> _bulkSender;

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
        _bulkSender = new BulkSender<SinkRecord, SinkRecord>(new BulkSenderOptions<SinkRecord, SinkRecord>
        {
            Transport = _transport,
            Action = ResolveAction,
            Body = static r => r,
            BodyTypeInfo = BulkDocumentConverter.CreateTypeInfo(options.SerializerOptions),
            Retry = BulkRetryPolicy.None, // Wallaby's SinkDispatcher owns retry/backoff; one request per call.
            ApplyLibrarySerializerDefaults = false, // The defaults re-resolve the body type, which needs a real resolver.
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
        // Chunks are sent sequentially so commit order is preserved across requests.
        foreach (var chunk in batch.Records.Chunk(_options.MaxRecordsPerRequest))
        {
            BulkResponse response;
            try
            {
                // Serialization runs synchronously, so an unencodable value or unresolvable destination throws here.
                response = await _bulkSender.SendAsync(chunk, ct);
            }
            catch (TransportException ex)
            {
                return DeliveryResult.Retry($"Elasticsearch bulk request failed: {ex.Message}", ex);
            }
            catch (Exception ex) when (ex is not WallabyConfigurationException and not OperationCanceledException)
            {
                return DeliveryResult.Permanent($"Elasticsearch bulk serialization failed: {ex.Message}", ex);
            }

            var failure = ClassifyResponse(chunk, response);
            if (failure is not null)
            {
                return failure;
            }
        }

        return DeliveryResult.Success;
    }

    /// <summary>Classify one chunk's bulk response; null on success, otherwise the classified failure.</summary>
    private static DeliveryResult? ClassifyResponse(SinkRecord[] chunk, BulkResponse response)
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

        if (response.Errors is not true || response.Items is null)
        {
            return null;
        }

        var items = ToItemResults(response.Items, chunk);
        return BulkJson.ClassifyItems(items, "Elasticsearch");
    }

    private static IEnumerable<BulkItemResult> ToItemResults(IEnumerable<BulkResponseItem> items, SinkRecord[] chunk)
    {
        // Response items line up positionally with the sent records, which supply the ids failure messages name.
        foreach (var (item, record) in items.Zip(chunk))
        {
            var error = item.Error is { } cause ? $"{cause.Type}: {cause.Reason}" : null;
            yield return new BulkItemResult(item.Action, item.Status, record.DocumentId, error);
        }
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
