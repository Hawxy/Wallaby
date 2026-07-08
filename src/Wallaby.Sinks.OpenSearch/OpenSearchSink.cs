using System.Text.Json;
using OpenSearch.Client;
using OpenSearch.Net;
using Wallaby.Abstractions;
using Wallaby.Sinks.OpenSearch.Internal;

namespace Wallaby.Sinks.OpenSearch;

/// <summary>
/// A destination that keeps OpenSearch indexes in sync with Postgres changes via the <c>_bulk</c> API.
/// Upserts are indexed with <c>_id</c> set to the record's document id (so updates are idempotent), and
/// deletions remove by that same id. Records are routed to the index named by
/// <see cref="SinkRecord.Destination"/> (falling back to <see cref="OpenSearchSinkOptions.DefaultIndex"/>);
/// indexes are not created or configured by the sink — they auto-create on first write unless pre-created
/// with explicit settings/mappings.
/// </summary>
public sealed class OpenSearchSink : ISink, IDisposable
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

        // Chunks are sent sequentially so commit order is preserved across requests.
        for (var offset = 0; offset < records.Count; offset += _options.MaxActionsPerRequest)
        {
            var count = Math.Min(_options.MaxActionsPerRequest, records.Count - offset);

            byte[] payload;
            try
            {
                payload = BulkWriter.Write(Name, records, offset, count, _options.DefaultIndex, _options.SerializerOptions);
            }
            catch (Exception ex)
            {
                // A document value the bulk body can't encode (or a record with no resolvable index) is a
                // transform/configuration bug; retrying would never succeed.
                return DeliveryResult.Permanent($"OpenSearch bulk serialization failed: {ex.Message}", ex);
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
        var parameters = new BulkRequestParameters
        {
            RequestConfiguration = new RequestConfiguration
            {
                RequestTimeout = TimeSpan.FromMilliseconds(_options.TimeoutMs),
            },
        };
        if (_options.Refresh)
        {
            parameters.Refresh = Refresh.WaitFor;
        }

        StringResponse response;
        try
        {
            response = await _client.LowLevel.BulkAsync<StringResponse>(PostData.Bytes(payload), parameters, ct);
        }
        catch (Exception ex) when (ct.IsCancellationRequested)
        {
            // The transport wraps cancellation (UnexpectedOpenSearchClientException); honor the caller's token.
            throw new OperationCanceledException("OpenSearch bulk request was canceled.", ex, ct);
        }
        catch (OpenSearchClientException ex)
        {
            return DeliveryResult.Retry($"OpenSearch bulk request failed: {ex.Message}", ex);
        }

        // Cancellation can also surface as a failed response rather than a throw.
        ct.ThrowIfCancellationRequested();

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

        return ClassifyItems(response.Body);
    }

    /// <summary>
    /// Classify a 2xx bulk response: per-item failures are reported under <c>errors</c>/<c>items</c>.
    /// Deleting an already-absent document is success (deletes are idempotent under at-least-once delivery);
    /// throttling/server item failures are retryable (re-sending the whole chunk is safe — actions are
    /// idempotent by <c>_id</c>); other item rejections (mapping/parse) are permanent. A permanent item
    /// outweighs retryable ones.
    /// </summary>
    private static DeliveryResult? ClassifyItems(string body)
    {
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
            return DeliveryResult.Retry($"OpenSearch returned an unrecognized bulk response: {ex.Message}", ex);
        }

        return permanent > 0
            ? DeliveryResult.Permanent($"OpenSearch rejected {permanent} bulk action(s); first: {firstPermanent}")
            : retryable > 0
                ? DeliveryResult.Retry($"OpenSearch reported {retryable} retryable bulk action failure(s).")
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

    /// <inheritdoc />
    public void Dispose() => ((IDisposable)_settings).Dispose();
}
