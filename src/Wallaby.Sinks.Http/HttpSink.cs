using System.Buffers;
using System.IO.Compression;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Wallaby.Abstractions;
using Wallaby.Sinks.Http.Internal;

namespace Wallaby.Sinks.Http;

/// <summary>
/// A destination that POSTs batches of changes to an HTTP endpoint as a JSON envelope of upsert/delete
/// records. Requests are sent through an <see cref="IHttpClientFactory"/> named client (see
/// <see cref="ClientNameFor"/>), so authentication is configured on the client; the sink optionally signs
/// each body with HMAC-SHA256 (<see cref="SignatureHeader"/>). Delivery is at-least-once: receivers must
/// upsert/delete idempotently by record id.
/// </summary>
public sealed class HttpSink : ISink
{
    /// <summary>Request header carrying <c>sha256=&lt;lowercase hex HMAC-SHA256 of the body&gt;</c> when signing is enabled.</summary>
    public const string SignatureHeader = "X-Wallaby-Signature";

    private readonly HttpSinkOptions _options;
    private readonly IHttpClientFactory _factory;
    private readonly string _clientName;
    private readonly Uri _endpoint;
    private readonly byte[]? _signingKey;

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
        _signingKey = options.SigningSecret is null ? null : Encoding.UTF8.GetBytes(options.SigningSecret);
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
            var signature = _signingKey is null
                ? null
                : $"sha256={Convert.ToHexStringLower(HMACSHA256.HashData(_signingKey, buffer.WrittenSpan))}";
            var body = Compress(buffer);

            var failure = await PostAsync(client, body, signature, ct);
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
        HttpClient client, ReadOnlyMemory<byte> body, string? signature, CancellationToken ct)
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

            if (signature is not null)
            {
                request.Headers.TryAddWithoutValidation(SignatureHeader, signature);
            }

            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token);
            if (response.IsSuccessStatusCode)
            {
                return null;
            }

            var status = (int)response.StatusCode;
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
