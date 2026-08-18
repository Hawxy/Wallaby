using Dekaf.Producer;
using NSubstitute;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Kafka.Tests.Unit;

internal static class KafkaTestHelpers
{
    public const string SinkName = "kafka";

    /// <summary>A sink over a substituted producer that records every produced message and succeeds.</summary>
    public static (KafkaSink Sink, List<ProducerMessage<string, byte[]>> Produced) CreateSink(
        Action<KafkaSinkOptions>? configure = null)
    {
        var produced = new List<ProducerMessage<string, byte[]>>();
        var producer = Substitute.For<IKafkaProducer<string, byte[]>>();
        producer.ProduceAsync(Arg.Any<ProducerMessage<string, byte[]>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var message = call.ArgAt<ProducerMessage<string, byte[]>>(0);
                produced.Add(message);
                return ValueTask.FromResult(new RecordMetadata
                {
                    Topic = message.Topic,
                    Partition = 0,
                    Offset = produced.Count - 1,
                    Timestamp = DateTimeOffset.UnixEpoch,
                });
            });
        return (CreateSink(producer, configure), produced);
    }

    /// <summary>A sink over the given producer (for failure-injection tests).</summary>
    public static KafkaSink CreateSink(IKafkaProducer<string, byte[]> producer, Action<KafkaSinkOptions>? configure = null)
    {
        var options = new KafkaSinkOptions { BootstrapServers = "unused:9092" };
        configure?.Invoke(options);
        return new KafkaSink(SinkName, options, producer);
    }

    public static ChangeMetadata Meta(int commitIdx = 0, bool backfill = false, DateTimeOffset? timestamp = null,
        ulong lsn = 12345, ChangeAction action = ChangeAction.Insert, string? backfillRunId = null)
        => new("public", "products", action, timestamp, lsn, commitIdx, backfill, backfillRunId);

    public static SinkRecord Upsert(string id, IReadOnlyDictionary<string, object?> document,
        string? destination = "products", ChangeMetadata? metadata = null)
        => new(destination, id, document, false, metadata ?? Meta());

    public static SinkRecord Delete(string id, string? destination = "products", ChangeMetadata? metadata = null)
        => new(destination, id, null, true, metadata ?? Meta(action: ChangeAction.Delete));

    public static SinkBatch Batch(params SinkRecord[] records) => new(SinkName, records);
}
