using System.Buffers;
using System.IO.Compression;
using System.Net.Http.Headers;
using Wallaby.Abstractions;
using Wallaby.Sinks.Http.Internal;

namespace Wallaby.Sinks.Http;

/// <summary>
/// A destination that POSTs batches of changes to an HTTP endpoint as a JSON envelope of upsert/delete
/// records. Requests are sent through an <see cref="IHttpClientFactory"/> named client (see
/// <see cref="ClientNameFor"/>), so authentication is configured on the client; the sink optionally signs
/// each request per the <a href="https://www.standardwebhooks.com/">Standard Webhooks</a> specification
/// (<see cref="SignatureHeader"/>). Delivery is at-least-once: receivers must upsert/delete
/// idempotently by record id.
/// </summary>
public sealed class HttpSink : ISink
{
    /// <summary>
    /// Request header carrying the message id when signing is enabled: <c>msg_</c> plus a hash of the
    /// delivered records' idempotency keys, so a retried delivery of the same records carries the same
    /// id and receivers can use it as a request-level idempotency key.
    /// </summary>
    public const string IdHeader = "webhook-id";

    /// <summary>
    /// Request header carrying the Unix-seconds timestamp bound into the signature; sent whenever
    /// signing is enabled. Receivers should reject requests whose timestamp falls outside their
    /// replay-tolerance window.
    /// </summary>
    public const string TimestampHeader = "webhook-timestamp";

    /// <summary>
    /// Request header carrying one or more space-delimited <c>v1,&lt;base64&gt;</c> signatures when
    /// signing is enabled: the HMAC-SHA256 of <c>{id}.{timestamp}.{body}</c> per Standard Webhooks,
    /// over the uncompressed request body. Two signatures are sent while
    /// <see cref="HttpSinkOptions.PreviousSigningSecret"/> rotates a key out.
    /// </summary>
    public const string SignatureHeader = "webhook-signature";

    private readonly HttpSinkOptions _options;
    private readonly IHttpClientFactory _factory;
    private readonly string _clientName;
    private readonly Uri _endpoint;
    private readonly WebhookSigner? _signer;

    /// <summary>
    /// Creates a sink that delivers to <see cref="HttpSinkOptions.Endpoint"/>. A client is drawn from
    /// <paramref name="factory"/> per delivery, so named-client configuration (handlers, headers,
    /// lifetimes) applies without caching a client on this long-lived sink.
    /// </summary>
    /// <param name="name">The sink's registration name (used for routing, telemetry, and test replacement).</param>
    /// <param name="options">Endpoint, signing, and delivery-behaviour settings.</param>
    /// <param name="factory">Factory providing the named <see cref="HttpClient"/>.</param>
    public HttpSink(string name, HttpSinkOptions options, IHttpClientFactory factory)
    {
        Name = name;
        _options = options;
        _factory = factory;
        _clientName = options.HttpClientName ?? ClientNameFor(name);
        _endpoint = new Uri(options.Endpoint, UriKind.Absolute);
        _signer = WebhookSigner.Create(name, options);
    }

    /// <summary>
    /// The default <see cref="IHttpClientFactory"/> client name for a sink, <c>wallaby.sinks.http.&lt;name&gt;</c>.
    /// Configure authentication on it: <c>services.AddHttpClient(HttpSink.ClientNameFor("webhook")).AddHttpMessageHandler(...)</c>.
    /// </summary>
    public static string ClientNameFor(string sinkName) => $"wallaby.sinks.http.{sinkName}";

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        var client = _factory.CreateClient(_clientName);
        var records = batch.Records;
        // One buffer serves every chunk of this call — the previous request has fully settled before the
        // next chunk resets it. Per-call (not per-instance) because ISink has no threading contract.
        var buffer = new ArrayBufferWriter<byte>();

        // Chunks are sent sequentially so commit order is preserved across requests.
        for (var offset = 0; offset < records.Count; offset += _options.MaxRecordsPerRequest)
        {
            var count = Math.Min(_options.MaxRecordsPerRequest, records.Count - offset);

            buffer.ResetWrittenCount();
            try
            {
                EnvelopeWriter.Write(
                    buffer, batch.SinkName, records, offset, count, _options.Annotations, _options.SerializerOptions);
            }
            catch (Exception ex)
            {
                // A document value the envelope can't encode is a transform/configuration bug; retrying
                // would never succeed.
                return DeliveryResult.Permanent($"HTTP sink envelope serialization failed: {ex.Message}", ex);
            }

            // Signed before compression, so receivers verify against the payload they read after
            // (middleware) decompression.
            var signing = _signer?.Sign(batch, offset, count, buffer.WrittenSpan);
            var body = Compress(buffer);

            var failure = await PostAsync(client, body, signing, ct);
            if (failure is not null)
            {
                return failure;
            }
        }

        return DeliveryResult.Success;
    }

    /// <summary>Apply the configured request-body compression; the payload as-is for <see cref="HttpSinkCompression.None"/>.</summary>
    private ReadOnlyMemory<byte> Compress(ArrayBufferWriter<byte> payload)
    {
        if (_options.Compression == HttpSinkCompression.None)
        {
            return payload.WrittenMemory;
        }

        using var output = new MemoryStream();
        using (Stream compressor = _options.Compression == HttpSinkCompression.Gzip
            ? new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true)
            : new BrotliStream(output, CompressionLevel.Fastest, leaveOpen: true))
        {
            compressor.Write(payload.WrittenSpan);
        }
        // The compressed bytes are handed off without the ToArray copy; the MemoryStream's buffer stays
        // reachable through the returned memory.
        return output.GetBuffer().AsMemory(0, (int)output.Length);
    }

    /// <summary>POST one envelope; null on success, otherwise the classified failure.</summary>
    private async Task<DeliveryResult?> PostAsync(
        HttpClient client, ReadOnlyMemory<byte> body, WebhookHeaders? signing, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(_options.TimeoutMs);

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint);
            var content = new ReadOnlyMemoryContent(body);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            if (_options.Compression != HttpSinkCompression.None)
            {
                content.Headers.ContentEncoding.Add(_options.Compression == HttpSinkCompression.Gzip ? "gzip" : "br");
            }
            request.Content = content;

            if (signing is { } headers)
            {
                request.Headers.TryAddWithoutValidation(IdHeader, headers.Id);
                request.Headers.TryAddWithoutValidation(TimestampHeader, headers.Timestamp);
                request.Headers.TryAddWithoutValidation(SignatureHeader, headers.Signature);
            }

            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token);
            if (response.IsSuccessStatusCode)
            {
                // A user-supplied client may still follow redirects; the response then comes from a URI
                // the request wasn't sent to, with the POST body dropped along the way. Compared against
                // the request's own (post-send) URI, not the configured endpoint, so a URI-rewriting
                // handler (service discovery, proxies) doesn't false-positive.
                var finalUri = response.RequestMessage?.RequestUri;
                if (finalUri is not null && request.RequestUri is not null && finalUri != request.RequestUri)
                {
                    return DeliveryResult.Permanent(
                        $"HTTP sink request to {request.RequestUri} was redirected to {finalUri} and the POST body " +
                        "was dropped in transit; the 2xx acknowledges nothing. Point Endpoint at the final URL, " +
                        "or disable redirect following on the custom HttpClient.");
                }
                return null;
            }

            var status = (int)response.StatusCode;
            if (status is >= 300 and < 400)
            {
                var location = response.Headers.Location?.ToString() ?? "an unspecified location";
                return DeliveryResult.Permanent(
                    $"HTTP sink request to {_endpoint} was answered with a redirect ({status}) to {location}. " +
                    "Following it would drop the POST body, so redirects are never followed; point Endpoint " +
                    "at the final URL.");
            }
            return status is 408 or 429 or >= 500
                ? DeliveryResult.Retry($"HTTP sink received {status} from {_endpoint}.")
                : DeliveryResult.Permanent($"HTTP sink request was rejected with {status} by {_endpoint}.");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            // The linked per-request timeout fired (also surfaces as TaskCanceledException from HttpClient).
            return DeliveryResult.Retry($"HTTP sink request to {_endpoint} timed out after {_options.TimeoutMs}ms.");
        }
        catch (HttpRequestException ex)
        {
            return DeliveryResult.Retry($"HTTP sink delivery failed: {ex.Message}", ex);
        }
    }
}
