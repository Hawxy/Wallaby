using System.Text.Json;
using OpenSearch.Client;

namespace Wallaby.Sinks.OpenSearch;

/// <summary>Configuration for a <see cref="OpenSearchSink"/>.</summary>
public sealed class OpenSearchSinkOptions
{
    /// <summary>OpenSearch base URL, e.g. <c>https://localhost:9200</c>.</summary>
    public required string Endpoint { get; set; }

    /// <summary>Basic-auth username. Null for an unsecured cluster.</summary>
    public string? Username { get; set; }

    /// <summary>Basic-auth password.</summary>
    public string? Password { get; set; }

    /// <summary>
    /// Full override for building the client's connection settings from <see cref="Endpoint"/> — use it for
    /// AWS SigV4 (the <c>OpenSearch.Net.Auth.AwsSigV4</c> connection), client certificates, connection pools,
    /// or proxies. When set, <see cref="Username"/>/<see cref="Password"/> are ignored; configure all
    /// authentication on the returned settings.
    /// </summary>
    public Func<Uri, ConnectionSettings>? ConfigureConnection { get; set; }

    /// <summary>Default index used when a routed record has no explicit destination.</summary>
    public string? DefaultIndex { get; set; }

    /// <summary>
    /// Maximum actions per <c>_bulk</c> request. Larger batches are split into sequential requests,
    /// preserving commit order.
    /// </summary>
    public int MaxActionsPerRequest { get; set; } = 500;

    /// <summary>Per-request timeout in milliseconds.</summary>
    public int TimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// When true, bulk requests use <c>refresh=wait_for</c> so documents are searchable before the batch is
    /// acknowledged. The default leaves visibility to the index's refresh interval (documents are durable
    /// either way).
    /// </summary>
    public bool Refresh { get; set; }

    /// <summary>
    /// Serializer for document values beyond the natively written scalar types. On NativeAOT hosts,
    /// point <see cref="JsonSerializerOptions.TypeInfoResolver"/> at a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> covering the value types your
    /// transforms emit; without it, non-scalar values fail delivery permanently on AOT.
    /// </summary>
    public JsonSerializerOptions? SerializerOptions { get; set; }
}
