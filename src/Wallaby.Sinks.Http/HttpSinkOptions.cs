using System.Text.Json;

namespace Wallaby.Sinks.Http;

/// <summary>Configuration for a <see cref="HttpSink"/>.</summary>
public sealed class HttpSinkOptions
{
    /// <summary>Absolute URL every envelope is POSTed to, e.g. <c>https://api.example.com/wallaby</c>.</summary>
    public required string Endpoint { get; set; }

    /// <summary>
    /// Name of the <see cref="IHttpClientFactory"/> client used for delivery; defaults to
    /// <see cref="HttpSink.ClientNameFor"/> of the sink name. Configure authentication (message
    /// handlers, default headers, client certificates, proxies) on that named client via
    /// <c>services.AddHttpClient(...)</c>.
    /// </summary>
    public string? HttpClientName { get; set; }

    /// <summary>
    /// Secret for HMAC-SHA256 body signing. When set, every request carries
    /// <c>X-Wallaby-Signature: sha256=&lt;lowercase hex&gt;</c> computed over the exact request body,
    /// so receivers can verify authenticity and integrity.
    /// </summary>
    public string? SigningSecret { get; set; }

    /// <summary>
    /// Compression applied to each request body (<c>Content-Encoding</c> is set accordingly). The
    /// receiver must support the encoding — e.g. ASP.NET Core's request decompression middleware.
    /// The HMAC signature always covers the uncompressed payload.
    /// </summary>
    public HttpSinkCompression Compression { get; set; } = HttpSinkCompression.None;

    /// <summary>
    /// Static key/value pairs echoed at the top level of every envelope — useful for receivers fed by
    /// several pipelines or environments (e.g. <c>{"env": "prod"}</c>).
    /// </summary>
    public IReadOnlyDictionary<string, string>? Annotations { get; set; }

    /// <summary>
    /// Maximum records per request. Larger batches are split into sequential requests, preserving
    /// commit order.
    /// </summary>
    public int MaxRecordsPerRequest { get; set; } = 500;

    /// <summary>
    /// Per-request timeout in milliseconds. Enforced with a linked cancellation, so it composes with
    /// any <see cref="HttpClient.Timeout"/> configured on the named client (whichever fires first).
    /// </summary>
    public int TimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// Serializer for document values beyond the natively written scalar types. On NativeAOT hosts,
    /// point <see cref="JsonSerializerOptions.TypeInfoResolver"/> at a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> covering the value types your
    /// transforms emit; without it, non-scalar values fail delivery permanently on AOT.
    /// </summary>
    public JsonSerializerOptions? SerializerOptions { get; set; }
}
