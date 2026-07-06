namespace Wallaby.Sinks.Http;

/// <summary>Request-body compression applied to each envelope.</summary>
public enum HttpSinkCompression
{
    /// <summary>Send the JSON payload as-is.</summary>
    None,

    /// <summary>Gzip the payload and send <c>Content-Encoding: gzip</c>.</summary>
    Gzip,

    /// <summary>Brotli-compress the payload and send <c>Content-Encoding: br</c>.</summary>
    Brotli,
}
