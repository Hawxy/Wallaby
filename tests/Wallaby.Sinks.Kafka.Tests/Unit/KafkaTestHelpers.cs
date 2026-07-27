using Confluent.Kafka;
using NSubstitute;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Kafka.Tests.Unit;

internal static class KafkaTestHelpers
{
    public const string SinkName = "kafka";

    /// <summary>A sink over a substituted producer that records every produced message and succeeds.</summary>
    public static (KafkaSink Sink, List<(string Topic, Message<string, byte[]> Message)> Produced) CreateSink(
        Action<KafkaSinkOptions>? configure = null)
    {
        var produced = new List<(string, Message<string, byte[]>)>();
        var producer = Substitute.For<IProducer<string, byte[]>>();
        producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, byte[]>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                produced.Add((call.ArgAt<string>(0), call.ArgAt<Message<string, byte[]>>(1)));
                return Task.FromResult(new Confluent.Kafka.DeliveryResult<string, byte[]>());
            });
        return (CreateSink(producer, configure), produced);
    }

    /// <summary>A sink over the given producer (for failure-injection tests).</summary>
    public static KafkaSink CreateSink(IProducer<string, byte[]> producer, Action<KafkaSinkOptions>? configure = null)
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
