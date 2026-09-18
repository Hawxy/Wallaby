namespace Wallaby.Sinks.Kafka;

/// <summary>Compression applied to the message batches the Kafka sink produces.</summary>
public enum KafkaSinkCompression
{
    /// <summary>No compression.</summary>
    None,

    /// <summary>Gzip.</summary>
    Gzip,

    /// <summary>Snappy.</summary>
    Snappy,

    /// <summary>LZ4, the default.</summary>
    Lz4,

    /// <summary>Zstandard.</summary>
    Zstd,
}
