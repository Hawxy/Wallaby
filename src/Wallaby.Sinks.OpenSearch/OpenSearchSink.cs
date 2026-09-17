using System.Buffers;
using OpenSearch.Client;
using OpenSearch.Net;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.OpenSearch;

/// <summary>
/// A destination that keeps OpenSearch indexes in sync with Postgres changes via the <c>_bulk</c> API.
/// Upserts are indexed with <c>_id</c> set to the record's document id (so updates are idempotent), and
/// deletions remove by that same id. Records are routed to the index named by
/// <see cref="SinkRecord.Destination"/> (falling back to <see cref="OpenSearchSinkOptions.DefaultIndex"/>);
/// indexes are not created or configured by the sink: they auto-create on first write unless pre-created
/// with explicit settings/mappings. A purge empties an index with <c>_delete_by_query</c>.
/// </summary>
public sealed class OpenSearchSink : ISink, ISinkPurger, IDisposable
{
    private readonly OpenSearchSinkOptions _options;
    private readonly ConnectionSettings _settings;
    private readonly IOpenSearchClient _client;

    /// <summary>
    /// Creates a sink that delivers to the OpenSearch cluster described by <paramref name="options"/>.
    /// The underlying client (and its connection pool) is created once and reused for the lifetime of
    /// the sink.
    /// </summary>
    /// <param name="name">The sink's registration name (used for routing, telemetry, and test replacement).</param>
    /// <param name="options">Connection, routing, and delivery-behaviour settings.</param>
    public OpenSearchSink(string name, OpenSearchSinkOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);
        OpenSearchBuilderExtensions.Validate(options);
        Name = name;
        _options = options;
        var endpoint = new Uri(options.Endpoint, UriKind.Absolute);
        _settings = options.ConfigureConnection is not null
            ? options.ConfigureConnection(endpoint)
            : BuildSettings(endpoint, options);
        _client = new OpenSearchClient(_settings);
    }

    private static ConnectionSettings BuildSettings(Uri endpoint, OpenSearchSinkOptions options)
    {
        var settings = new ConnectionSettings(endpoint);
        if (options.Username is not null)
        {
            settings.BasicAuthentication(options.Username, options.Password);
        }
        return settings;
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        var records = batch.Records;
        // One buffer serves every chunk of this call; the previous request has fully settled before the
        // next chunk resets it.
        var buffer = new ArrayBufferWriter<byte>();

        // Chunks are sent sequentially so commit order is preserved across requests.
        for (var offset = 0; offset < records.Count; offset += _options.MaxRecordsPerRequest)
        {
            var count = Math.Min(_options.MaxRecordsPerRequest, records.Count - offset);

            buffer.ResetWrittenCount();
            try
            {
                BulkJson.Write(buffer, Name, records, offset, count, _options.DefaultIndex, _options.SerializerOptions);
            }
            catch (Exception ex) when (ex is not WallabyConfigurationException)
            {
                // A document value the bulk body can't encode is a transform bug; retrying would never succeed.
                return DeliveryResult.Permanent($"OpenSearch bulk serialization failed: {ex.Message}", ex);
            }

            var failure = await SendAsync(buffer.WrittenMemory, ct);
            if (failure is not null)
            {
                return failure;
            }
        }

        return DeliveryResult.Success;
    }

    /// <inheritdoc />
    public async Task PurgeAsync(SinkPurgeRequest request, CancellationToken ct)
    {
        var index = SinkDestination.Resolve(request, _options.DefaultIndex, Name, nameof(_options.DefaultIndex));
        var parameters = new DeleteByQueryRequestParameters
        {
            Conflicts = Conflicts.Proceed,
            Refresh = Refresh.True,
            RequestConfiguration = RequestConfig(),
        };

        var response = await _client.LowLevel.DeleteByQueryAsync<BytesResponse>(
            index, PostData.ReadOnlyMemory(BulkJson.MatchAllQuery), parameters, ct);

        var status = response.HttpStatusCode;
        if (status == 404)
        {
            return; // The index was never written: nothing to purge.
        }
        if (status is null or < 200 or >= 300 || response.OriginalException is not null)
        {
            throw new InvalidOperationException(
                $"OpenSearch purge of index '{index}' failed: " +
                (response.OriginalException?.Message ?? $"status {status}"),
                response.OriginalException);
        }
        if (BulkJson.DescribeDeleteByQueryFailure(response.Body) is { } failure)
        {
            throw new InvalidOperationException($"OpenSearch purge of index '{index}' failed: {failure}");
        }
    }

    private RequestConfiguration RequestConfig() => new() { RequestTimeout = _options.Timeout };

    /// <summary>Send one bulk body; null on success, otherwise the classified failure.</summary>
    private async Task<DeliveryResult?> SendAsync(ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        var parameters = new BulkRequestParameters { RequestConfiguration = RequestConfig() };
        if (_options.Refresh)
        {
            parameters.Refresh = Refresh.WaitFor;
        }

        BytesResponse response;
        try
        {
            response = await _client.LowLevel.BulkAsync<BytesResponse>(PostData.ReadOnlyMemory(payload), parameters, ct);
        }
        catch (OpenSearchClientException ex)
        {
            return DeliveryResult.Retry($"OpenSearch bulk request failed: {ex.Message}", ex);
        }

        var status = response.HttpStatusCode;
        if (status is null)
        {
            // No HTTP status: DNS/socket failure or the per-request timeout.
            return DeliveryResult.Retry(
                $"OpenSearch bulk request failed: {response.OriginalException?.Message ?? "no response"}",
                response.OriginalException);
        }

        if (status is 408 or 429 or >= 500)
        {
            return DeliveryResult.Retry($"OpenSearch bulk request received {status}.");
        }

        if (status is < 200 or >= 300)
        {
            return DeliveryResult.Permanent($"OpenSearch bulk request was rejected with {status}.");
        }

        if (response.OriginalException is not null)
        {
            // A transport failure can surface with a default status; never treat it as an applied bulk.
            return DeliveryResult.Retry(
                $"OpenSearch bulk request failed: {response.OriginalException.Message}", response.OriginalException);
        }

        return BulkJson.ClassifyItems(response.Body, "OpenSearch");
    }

    /// <inheritdoc />
    public void Dispose() => ((IDisposable)_settings).Dispose();
}
