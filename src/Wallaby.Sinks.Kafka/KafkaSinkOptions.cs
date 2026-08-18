using System.Text.Json;
using Dekaf;
using Dekaf.Protocol.Records;

namespace Wallaby.Sinks.Kafka;

/// <summary>Configuration for a <see cref="KafkaSink"/>.</summary>
public sealed class KafkaSinkOptions
{
    /// <summary>Comma-separated Kafka bootstrap servers, e.g. <c>broker-1:9092,broker-2:9092</c>.</summary>
    public required string BootstrapServers { get; set; }

    /// <summary>
    /// Topic for records whose mapping declares no destination. A record with neither fails delivery
    /// permanently (a configuration bug).
    /// </summary>
    public string? DefaultTopic { get; set; }

    /// <summary>
    /// Topics to create on startup (see <see cref="KafkaSink.InitializeAsync"/>); topics that already
    /// exist are left untouched. Empty (the default) skips topic creation entirely — brokers with
    /// <c>auto.create.topics.enable</c> or pre-provisioned topics need nothing here.
    /// </summary>
    public IList<KafkaTopicConfig> Topics { get; } = [];

    /// <summary>
    /// Connection-level settings applied to the shared Kafka client behind both the producer and the
    /// topic-creating admin client: TLS (<c>UseTls</c>), SASL (<c>WithSaslPlain</c>,
    /// <c>WithSaslScramSha512</c>, OAuth bearer, AWS MSK IAM), connection timeouts, and DNS behaviour.
    /// </summary>
    public Action<KafkaClientBuilder>? ConfigureClient { get; set; }

    /// <summary>
    /// Producer settings the sink does not wrap (batch size, retry policy, socket buffers). Runs after
    /// the sink's own producer configuration, so settings here win on conflict, except idempotence and
    /// acks: the sink reapplies those afterwards because at-least-once delivery depends on them.
    /// </summary>
    public Action<ProducerBuilder<string, byte[]>>? ConfigureProducer { get; set; }

    /// <summary>Compression applied to produced message batches.</summary>
    public CompressionType Compression { get; set; } = CompressionType.Lz4;

    /// <summary>How long the producer lingers to fill a batch before sending, in milliseconds.</summary>
    public int LingerMs { get; set; } = 5;

    /// <summary>
    /// Per-message delivery ceiling in milliseconds: how long the producer retries transient broker
    /// errors internally before the failure surfaces to the dispatcher as retryable.
    /// </summary>
    public int MessageTimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// Ceiling in milliseconds on creating <see cref="Topics"/> at initialization, so an unreachable
    /// broker fails the leader session (which retries with backoff) instead of stalling startup.
    /// </summary>
    public int AdminTimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// Static key/value pairs echoed in every message value — useful for consumers fed by several
    /// pipelines or environments (e.g. <c>{"env": "prod"}</c>).
    /// </summary>
    public IReadOnlyDictionary<string, string>? Annotations { get; set; }

    /// <summary>
    /// Serializer for document values beyond the natively written scalar types. On NativeAOT hosts,
    /// point <see cref="JsonSerializerOptions.TypeInfoResolver"/> at a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> covering the value types your
    /// transforms emit; without it, non-scalar values fail delivery permanently on AOT.
    /// </summary>
    public JsonSerializerOptions? SerializerOptions { get; set; }
}

/// <summary>A topic the sink creates on startup if it does not already exist.</summary>
public sealed class KafkaTopicConfig
{
    /// <summary>Topic name.</summary>
    public required string Name { get; set; }

    /// <summary>Partition count. More partitions increase parallelism but only per-partition order is guaranteed.</summary>
    public int Partitions { get; set; } = 1;

    /// <summary>Replication factor; <c>-1</c> uses the broker default.</summary>
    public short ReplicationFactor { get; set; } = -1;

    /// <summary>
    /// Topic-level settings, e.g. <c>{"cleanup.policy": "compact"}</c> — recommended for entity topics,
    /// where the latest message per document id is the document's current state and deletes are tombstones.
    /// </summary>
    public IDictionary<string, string> Config { get; } = new Dictionary<string, string>(StringComparer.Ordinal);
}
