using Dekaf;
using Dekaf.Admin;
using Dekaf.Compression.Lz4;
using Dekaf.Compression.Snappy;
using Dekaf.Compression.Zstd;
using Dekaf.Errors;
using Dekaf.Producer;
using Dekaf.Protocol;
using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Sinks.Kafka.Internal;
using CompressionType = Dekaf.Protocol.Records.CompressionType;
using DeliveryResult = Wallaby.Abstractions.DeliveryResult;

namespace Wallaby.Sinks.Kafka;

/// <summary>
/// A destination that produces changes to Kafka topics. Records are routed to the topic named by
/// <see cref="SinkRecord.Destination"/> (falling back to <see cref="KafkaSinkOptions.DefaultTopic"/>),
/// keyed by <see cref="SinkRecord.DocumentId"/> so all changes to a document land on one partition in
/// commit order. Upserts carry a self-contained JSON envelope; deletions are tombstones (null value,
/// same key), so a compacted topic converges to each document's latest state. Delivery is at-least-once:
/// the producer runs idempotent with <c>acks=all</c>, and a batch is only reported delivered once every
/// message's delivery report has succeeded — consumers deduplicate replays by the
/// <c>wallaby.idempotency-key</c> header (or let compaction absorb them).
/// </summary>
public sealed class KafkaSink : ISink, ISinkInitializer, IAsyncDisposable
{
    private readonly KafkaSinkOptions _options;
    private readonly ILoggerFactory? _loggerFactory;

    // Lazy initialization is unsynchronized by contract: ISink guarantees deliveries are serialized
    // per sink and initialization completes before streaming starts.
    private KafkaClient? _client;
    private IKafkaProducer<string, byte[]>? _producer;

    /// <summary>
    /// Creates a sink that produces to the cluster described by <paramref name="options"/>. The
    /// underlying client (and its broker connections) is created on first use — not at registration
    /// time — reused for the lifetime of the sink, and flushed and released when the sink is disposed.
    /// </summary>
    /// <param name="name">The sink's registration name (used for routing, telemetry, and test replacement).</param>
    /// <param name="options">Cluster, topic, and delivery-behaviour settings.</param>
    /// <param name="loggerFactory">Receives the Kafka client's internal logs; pass the host's factory to surface them.</param>
    public KafkaSink(string name, KafkaSinkOptions options, ILoggerFactory? loggerFactory = null)
        : this(name, options, producer: null, loggerFactory)
    {
    }

    internal KafkaSink(string name, KafkaSinkOptions options, IKafkaProducer<string, byte[]>? producer, ILoggerFactory? loggerFactory = null)
    {
        Name = name;
        _options = options;
        _producer = producer;
        _loggerFactory = loggerFactory;
    }

    // The client is shared by the producer and the topic-creating admin client, so connection-level
    // settings (bootstrap, TLS, SASL) are configured once and broker connections are pooled.
    private KafkaClient GetClient()
    {
        if (_client is null)
        {
            var builder = new KafkaClientBuilder()
                .WithBootstrapServers(_options.BootstrapServers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
            if (_loggerFactory is not null)
            {
                builder.WithLoggerFactory(_loggerFactory);
            }
            _options.ConfigureClient?.Invoke(builder);
            _client = builder.Build();
        }
        return _client;
    }

    private async ValueTask<IKafkaProducer<string, byte[]>> GetProducerAsync(CancellationToken ct)
    {
        if (_producer is not null)
        {
            return _producer;
        }

        // The delivery timeout must cover at least one full request plus the linger window; derive the
        // per-request bound from the configured ceiling so any valid MessageTimeoutMs builds.
        var requestTimeoutMs = Math.Min(30_000, _options.MessageTimeoutMs - _options.LingerMs);

        var builder = GetClient().CreateProducer<string, byte[]>()
            .WithLinger(TimeSpan.FromMilliseconds(_options.LingerMs))
            .WithRequestTimeout(TimeSpan.FromMilliseconds(requestTimeoutMs))
            .WithDeliveryTimeout(TimeSpan.FromMilliseconds(_options.MessageTimeoutMs))
            // A produce call also blocks on metadata/buffer space before enqueueing; bound that wait by
            // the same ceiling so every delivery failure surfaces within MessageTimeoutMs.
            .WithMaxBlock(TimeSpan.FromMilliseconds(_options.MessageTimeoutMs));
        ApplyCompression(builder, _options.Compression);
        _options.ConfigureProducer?.Invoke(builder);

        // Applied after ConfigureProducer so the callback cannot weaken them. Idempotent + acks=all:
        // broker-side dedup of the producer's internal retries and no loss on broker failover, while
        // preserving per-partition produce order; the slot only advances on that guarantee.
        builder
            .WithIdempotence(true)
            .WithAcks(Acks.All);
        return _producer = await builder.BuildAsync(ct);
    }

    // Lz4/Zstd/Snappy also register their codec, so selecting them is a single call per type.
    private static void ApplyCompression(ProducerBuilder<string, byte[]> builder, CompressionType compression)
    {
        switch (compression)
        {
            case CompressionType.None:
                break;
            case CompressionType.Gzip:
                builder.UseGzipCompression();
                break;
            case CompressionType.Lz4:
                builder.UseLz4Compression();
                break;
            case CompressionType.Zstd:
                builder.UseZstdCompression();
                break;
            case CompressionType.Snappy:
                builder.UseSnappyCompression();
                break;
            default:
                throw new NotSupportedException($"Compression type '{compression}' is not supported by the Kafka sink.");
        }
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <summary>
    /// Create the topics declared in <see cref="KafkaSinkOptions.Topics"/>; existing topics are left
    /// untouched. Runs on the leader before streaming and again on every leadership takeover.
    /// </summary>
    public async Task InitializeAsync(CancellationToken ct)
    {
        if (_options.Topics.Count == 0)
        {
            return;
        }

        // One deadline across all topics: the admin client retries transient failures internally, so an
        // unreachable broker would otherwise stall the leader session well past AdminTimeoutMs.
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(_options.AdminTimeoutMs);

        try
        {
            await using var admin = GetClient().CreateAdminClient().Build();
            var createOptions = new CreateTopicsOptions { TimeoutMs = _options.AdminTimeoutMs };
            foreach (var topic in _options.Topics)
            {
                var spec = new NewTopic
                {
                    Name = topic.Name,
                    NumPartitions = topic.Partitions,
                    ReplicationFactor = topic.ReplicationFactor,
                    Configs = topic.Config.Count > 0 ? new Dictionary<string, string>(topic.Config) : null,
                };

                try
                {
                    // CreateTopicsAsync returns only once the topic's partitions have leaders, so
                    // streaming never starts against a topic still propagating.
                    await admin.CreateTopicsAsync([spec], createOptions, deadline.Token);
                }
                catch (KafkaException ex) when (ex.ErrorCode == ErrorCode.TopicAlreadyExists)
                {
                    // Created by a previous leader session or another node; topics are created one at a
                    // time so this cannot mask a different topic's failure.
                }
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Kafka topic creation for sink '{Name}' timed out after {_options.AdminTimeoutMs}ms.");
        }
    }

    /// <inheritdoc />
    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        var records = batch.Records;
        if (records.Count == 0)
        {
            return DeliveryResult.Success;
        }

        // Validate and serialize the whole batch before the first produce, so a permanent failure can
        // never leave already-enqueued delivery reports behind.
        var messages = new ProducerMessage<string, byte[]>[records.Count];
        for (var i = 0; i < records.Count; i++)
        {
            var record = records[i];
            var topic = record.Destination ?? _options.DefaultTopic;
            if (topic is null)
            {
                return DeliveryResult.Permanent(
                    $"Record {record.DocumentId} has no destination and no DefaultTopic is configured for sink '{Name}'.");
            }

            byte[]? value;
            try
            {
                value = record.IsDeletion
                    ? null // Tombstone: compaction removes the document; headers still carry its context.
                    : KafkaMessageWriter.WriteValue(record, _options.Annotations, _options.SerializerOptions);
            }
            catch (Exception ex)
            {
                // A document value the envelope can't encode is a transform/configuration bug; retrying
                // would never succeed.
                return DeliveryResult.Permanent($"Kafka sink message serialization failed: {ex.Message}", ex);
            }

            messages[i] = new ProducerMessage<string, byte[]>
            {
                Topic = topic,
                Key = record.DocumentId,
                Value = value!, // null-forgiving: a tombstone genuinely carries a null value
                Headers = KafkaMessageWriter.BuildHeaders(record),
            };
        }

        // The produce loop runs inside the classified try: producer creation can fail transiently, and
        // ProduceAsync can throw synchronously — both must classify, not escape as raw exceptions the
        // dispatcher won't retry.
        var reports = new List<Task>(records.Count);
        try
        {
            // Produce calls are pipelined (each settles on delivery, not enqueue) and the producer
            // preserves produce order per partition, so per-document commit order survives batching and
            // retries.
            var producer = await GetProducerAsync(ct);
            for (var i = 0; i < messages.Length; i++)
            {
                reports.Add(producer.ProduceAsync(messages[i], ct).AsTask());
            }

            // Every delivery report is awaited, so a batch is only reported delivered (and the LSN acked)
            // once the brokers have actually accepted all of it.
            await Task.WhenAll(reports);
            return DeliveryResult.Success;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (KafkaException ex)
        {
            await ObserveAsync(reports);
            return Classify(ex);
        }
    }

    // A produce call can throw before every report is enqueued; the in-flight reports still settle and
    // must be awaited so none surfaces as an unobserved task fault.
    private static async Task ObserveAsync(List<Task> reports)
    {
        try
        {
            await Task.WhenAll(reports);
        }
        catch
        {
        }
    }

    // The producer retries retriable broker errors internally until MessageTimeoutMs, so what surfaces
    // here is either a request the cluster will never accept or a transient condition worth retrying
    // from the dispatcher with backoff.
    private static DeliveryResult Classify(KafkaException exception)
    {
        // Timeouts and bootstrap DNS failures carry no protocol error code (their IsRetriable is false)
        // but are transient infrastructure conditions, not permanent rejections.
        var retry = exception is KafkaTimeoutException or BootstrapResolutionException || exception.IsRetriable;
        var description = exception.ErrorCode is { } code
            ? $"Kafka delivery failed ({code}): {exception.Message}"
            : $"Kafka delivery failed: {exception.Message}";
        return retry
            ? DeliveryResult.Retry(description, exception)
            : DeliveryResult.Permanent(description, exception);
    }

    /// <summary>Flushes in-flight messages and releases the client. Called by the runtime at host shutdown.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_producer is not null)
        {
            // Un-flushed messages belong to an unacknowledged batch and would be redelivered after
            // restart, but flushing avoids pointless redelivery on a clean shutdown.
            await _producer.DisposeAsync();
        }
        if (_client is not null)
        {
            await _client.DisposeAsync();
        }
    }
}
