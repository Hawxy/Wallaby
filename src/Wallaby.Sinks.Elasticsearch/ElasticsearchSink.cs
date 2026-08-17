using Elastic.Clients.Elasticsearch;
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
/// with explicit settings/mappings.
/// </summary>
public sealed class ElasticsearchSink : ISink, IDisposable
{
    private readonly ElasticsearchSinkOptions _options;
    private readonly ElasticsearchClientSettings _settings;
    private readonly ElasticsearchClient _client;

    /// <summary>
    /// Creates a sink that delivers to the Elasticsearch cluster described by <paramref name="options"/>.
    /// The underlying client (and its connection pool) is created once and reused for the lifetime of
    /// the sink.
    /// </summary>
    /// <param name="name">The sink's registration name (used for routing, telemetry, and test replacement).</param>
    /// <param name="options">Connection, routing, and delivery-behaviour settings.</param>
    public ElasticsearchSink(string name, ElasticsearchSinkOptions options)
    {
        Name = name;
        _options = options;
        var endpoint = new Uri(options.Endpoint, UriKind.Absolute);
        _settings = options.ConfigureConnection is not null
            ? options.ConfigureConnection(endpoint)
            : BuildSettings(endpoint, options);
        _client = new ElasticsearchClient(_settings);
    }

    private static ElasticsearchClientSettings BuildSettings(Uri endpoint, ElasticsearchSinkOptions options)
    {
        var settings = new ElasticsearchClientSettings(endpoint)
            .RequestTimeout(TimeSpan.FromMilliseconds(options.TimeoutMs));
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

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        var records = batch.Records;

        // Chunks are sent sequentially so commit order is preserved across requests.
        for (var offset = 0; offset < records.Count; offset += _options.MaxActionsPerRequest)
        {
            var count = Math.Min(_options.MaxActionsPerRequest, records.Count - offset);

            byte[] payload;
            try
            {
                payload = BulkJson.Write(Name, records, offset, count, _options.DefaultIndex, _options.SerializerOptions);
            }
            catch (Exception ex)
            {
                // A document value the bulk body can't encode (or a record with no resolvable index) is a
                // transform/configuration bug; retrying would never succeed.
                return DeliveryResult.Permanent($"Elasticsearch bulk serialization failed: {ex.Message}", ex);
            }

            var failure = await SendAsync(payload, ct);
            if (failure is not null)
            {
                return failure;
            }
        }

        return DeliveryResult.Success;
    }

    /// <summary>Send one bulk body; null on success, otherwise the classified failure.</summary>
    private async Task<DeliveryResult?> SendAsync(byte[] payload, CancellationToken ct)
    {
        var path = _options.Refresh ? "/_bulk?refresh=wait_for" : "/_bulk";

        StringResponse response;
        try
        {
            response = await _client.Transport.RequestAsync<StringResponse>(
                new EndpointPath(HttpMethod.POST, path), PostData.Bytes(payload), null, null, ct);
        }
        catch (Exception ex) when (ct.IsCancellationRequested)
        {
            // The transport can wrap cancellation; honor the caller's token.
            throw new OperationCanceledException("Elasticsearch bulk request was canceled.", ex, ct);
        }
        catch (TransportException ex)
        {
            return DeliveryResult.Retry($"Elasticsearch bulk request failed: {ex.Message}", ex);
        }

        // Cancellation can also surface as a failed response rather than a throw.
        ct.ThrowIfCancellationRequested();

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

        return BulkJson.ClassifyItems(response.Body, "Elasticsearch");
    }

    /// <inheritdoc />
    public void Dispose() => ((IDisposable)_settings).Dispose();
}
