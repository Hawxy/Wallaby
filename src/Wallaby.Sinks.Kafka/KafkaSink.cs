using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Wallaby.Abstractions;
using Wallaby.Sinks.Kafka.Internal;
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
public sealed class KafkaSink : ISink, ISinkInitializer, IDisposable
{
    private readonly KafkaSinkOptions _options;

    private IProducer<string, byte[]>? _producer;

    /// <summary>
    /// Creates a sink that produces to the cluster described by <paramref name="options"/>. The
    /// underlying producer (and its broker connections) is created on first delivery — not at
    /// registration time — reused for the lifetime of the sink, and flushed and released when the sink
    /// is disposed.
    /// </summary>
    /// <param name="name">The sink's registration name (used for routing, telemetry, and test replacement).</param>
    /// <param name="options">Cluster, topic, and delivery-behaviour settings.</param>
    public KafkaSink(string name, KafkaSinkOptions options)
        : this(name, options, producer: null)
    {
    }

    internal KafkaSink(string name, KafkaSinkOptions options, IProducer<string, byte[]>? producer)
    {
        Name = name;
        _options = options;
        _producer = producer;
    }

    private IProducer<string, byte[]> GetProducer()
        => _producer ??= new ProducerBuilder<string, byte[]>(BuildProducerConfig(_options)).Build();

    private static ProducerConfig BuildProducerConfig(KafkaSinkOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            // Idempotent + acks=all: broker-side dedup of librdkafka's internal retries and no loss on
            // broker failover, while preserving per-partition produce order.
            EnableIdempotence = true,
            Acks = Acks.All,
            CompressionType = options.Compression,
            LingerMs = options.LingerMs,
            MessageTimeoutMs = options.MessageTimeoutMs,
        };
        foreach (var setting in options.ClientConfig)
        {
            config.Set(setting.Key, setting.Value);
        }
        return config;
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

        var config = new AdminClientConfig { BootstrapServers = _options.BootstrapServers };
        foreach (var setting in _options.ClientConfig)
        {
            config.Set(setting.Key, setting.Value);
        }

        using var admin = new AdminClientBuilder(config).Build();
        var specs = _options.Topics
            .Select(t => new TopicSpecification
            {
                Name = t.Name,
                NumPartitions = t.Partitions,
                ReplicationFactor = t.ReplicationFactor,
                Configs = t.Config.Count > 0 ? new Dictionary<string, string>(t.Config) : null,
            })
            .ToList();

        // OperationTimeout waits for the topics to actually exist (not just be accepted), so streaming
        // never starts against topics still propagating.
        var timeout = TimeSpan.FromMilliseconds(_options.AdminTimeoutMs);
        var create = admin.CreateTopicsAsync(specs, new CreateTopicsOptions
        {
            RequestTimeout = timeout,
            OperationTimeout = timeout,
        });

        try
        {
            // CreateTopicsAsync has no cancellation overload; WaitAsync abandons it on cancellation.
            await create.WaitAsync(ct);
        }
        catch (CreateTopicsException ex) when (ex.Results.TrueForAll(
            r => r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
        {
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Observe the abandoned task's eventual fault so it never surfaces as an unobserved exception.
            _ = create.ContinueWith(
                static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
            throw;
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
        var topics = new string[records.Count];
        var messages = new Message<string, byte[]>[records.Count];
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

            topics[i] = topic;
            messages[i] = new Message<string, byte[]>
            {
                Key = record.DocumentId,
                Value = value!,
                Headers = KafkaMessageWriter.BuildHeaders(record),
            };
        }

        // The produce loop runs inside the classified try: ProduceAsync throws synchronously when the
        // local queue is full (a transient condition), and producer creation can fail transiently too —
        // both must classify, not escape as raw exceptions the dispatcher won't retry.
        var reports = new List<Task>(records.Count);
        try
        {
            // Produce calls are pipelined (each returns on enqueue, not delivery) and librdkafka preserves
            // produce order per partition, so per-document commit order survives batching and retries.
            var producer = GetProducer();
            for (var i = 0; i < messages.Length; i++)
            {
                reports.Add(producer.ProduceAsync(topics[i], messages[i], ct));
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
        catch (ProduceException<string, byte[]> ex)
        {
            await ObserveAsync(reports);
            return Classify(ex.Error, ex);
        }
        catch (KafkaException ex)
        {
            await ObserveAsync(reports);
            return Classify(ex.Error, ex);
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

    // librdkafka retries transient broker errors internally until MessageTimeoutMs, so what surfaces here
    // is either fatal to the producer, a request the cluster will never accept, or a timeout worth
    // retrying from the dispatcher with backoff.
    private static DeliveryResult Classify(Error error, Exception exception)
    {
        var permanent = error.IsFatal || error.Code is
            ErrorCode.MsgSizeTooLarge or
            ErrorCode.TopicAuthorizationFailed or
            ErrorCode.ClusterAuthorizationFailed or
            ErrorCode.SaslAuthenticationFailed or
            ErrorCode.InvalidMsg;
        var description = $"Kafka delivery failed ({error.Code}): {error.Reason}";
        return permanent
            ? DeliveryResult.Permanent(description, exception)
            : DeliveryResult.Retry(description, exception);
    }

    /// <summary>Flushes in-flight messages and releases the producer. Called by the runtime at host shutdown.</summary>
    public void Dispose()
    {
        if (_producer is null)
        {
            return;
        }

        // Un-flushed messages belong to an unacknowledged batch and would be redelivered after restart,
        // but flushing avoids pointless redelivery on a clean shutdown.
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
