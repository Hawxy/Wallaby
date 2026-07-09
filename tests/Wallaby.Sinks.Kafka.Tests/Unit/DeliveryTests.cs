using System.Text.Json;
using Confluent.Kafka;
using NSubstitute;
using Wallaby.Abstractions;
using Wallaby.Sinks.Kafka.Internal;
using static Wallaby.Sinks.Kafka.Tests.Unit.KafkaTestHelpers;

namespace Wallaby.Sinks.Kafka.Tests.Unit;

/// <summary>Delivery-loop behavior over a substituted producer: routing, ordering, tombstones, classification.</summary>
public class DeliveryTests
{
    [Test]
    public async Task Records_are_produced_in_order_keyed_by_document_id()
    {
        var (sink, produced) = CreateSink();
        var batch = Batch(
            Upsert("1", new Dictionary<string, object?> { ["name"] = "a" }),
            Upsert("2", new Dictionary<string, object?> { ["name"] = "b" }, destination: "other"),
            Delete("1"));

        var result = await sink.DeliverAsync(batch, CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        produced.Select(p => (p.Topic, p.Message.Key)).ShouldBe([("products", "1"), ("other", "2"), ("products", "1")]);
    }

    [Test]
    public async Task Deletions_are_tombstones()
    {
        var (sink, produced) = CreateSink();

        await sink.DeliverAsync(Batch(Delete("42")), CancellationToken.None);

        var message = produced.ShouldHaveSingleItem().Message;
        message.Value.ShouldBeNull();
        System.Text.Encoding.UTF8.GetString(message.Headers.GetLastBytes(KafkaMessageWriter.OperationHeader))
            .ShouldBe("delete");
    }

    [Test]
    public async Task Upsert_values_are_the_json_envelope()
    {
        var (sink, produced) = CreateSink();

        await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?> { ["name"] = "a" })), CancellationToken.None);

        using var envelope = JsonDocument.Parse(produced.Single().Message.Value);
        envelope.RootElement.GetProperty("operation").GetString().ShouldBe("upsert");
        envelope.RootElement.GetProperty("document").GetProperty("name").GetString().ShouldBe("a");
    }

    [Test]
    public async Task The_default_topic_is_used_when_a_record_has_no_destination()
    {
        var (sink, produced) = CreateSink(o => o.DefaultTopic = "fallback");

        await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>(), destination: null)), CancellationToken.None);

        produced.Single().Topic.ShouldBe("fallback");
    }

    [Test]
    public async Task A_record_with_no_topic_fails_permanently()
    {
        var (sink, produced) = CreateSink();

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>(), destination: null)), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error.ShouldNotBeNull().ShouldContain("DefaultTopic");
        produced.ShouldBeEmpty();
    }

    [Test]
    public async Task A_value_the_envelope_cannot_encode_fails_permanently()
    {
        var (sink, _) = CreateSink();

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?> { ["bad"] = typeof(int) })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error.ShouldNotBeNull().ShouldContain("serialization");
    }

    [Test]
    [Arguments(ErrorCode.MsgSizeTooLarge, DeliveryStatus.PermanentFailure)]
    [Arguments(ErrorCode.TopicAuthorizationFailed, DeliveryStatus.PermanentFailure)]
    [Arguments(ErrorCode.Local_MsgTimedOut, DeliveryStatus.RetryableFailure)]
    [Arguments(ErrorCode.UnknownTopicOrPart, DeliveryStatus.RetryableFailure)]
    [Arguments(ErrorCode.Local_Transport, DeliveryStatus.RetryableFailure)]
    public async Task Broker_errors_are_classified(ErrorCode code, DeliveryStatus expected)
    {
        var sink = CreateSink(FailingProducer(new Error(code)));

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(expected);
        result.Error.ShouldNotBeNull().ShouldContain(code.ToString());
    }

    [Test]
    public async Task Fatal_producer_errors_are_permanent()
    {
        var sink = CreateSink(FailingProducer(new Error(ErrorCode.OutOfOrderSequenceNumber, "fenced", isFatal: true)));

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
    }

    [Test]
    public async Task One_failed_report_fails_the_batch_after_all_reports_settle()
    {
        var settled = 0;
        var producer = Substitute.For<IProducer<string, byte[]>>();
        producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, byte[]>>(), Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromException<Confluent.Kafka.DeliveryResult<string, byte[]>>(
                    new ProduceException<string, byte[]>(
                        new Error(ErrorCode.Local_MsgTimedOut), new Confluent.Kafka.DeliveryResult<string, byte[]>())),
                async _ =>
                {
                    await Task.Delay(50);
                    Interlocked.Increment(ref settled);
                    return new Confluent.Kafka.DeliveryResult<string, byte[]>();
                });
        var sink = CreateSink(producer);

        var result = await sink.DeliverAsync(
            Batch(
                Upsert("1", new Dictionary<string, object?>()),
                Upsert("2", new Dictionary<string, object?>())),
            CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        settled.ShouldBe(1); // the slower in-flight report was still awaited before failing the batch
    }

    [Test]
    public async Task An_empty_batch_succeeds_without_producing()
    {
        var (sink, produced) = CreateSink();

        var result = await sink.DeliverAsync(Batch(), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        produced.ShouldBeEmpty();
    }

    [Test]
    public async Task Cancellation_propagates()
    {
        using var cts = new CancellationTokenSource();
        var producer = Substitute.For<IProducer<string, byte[]>>();
        producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, byte[]>>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                await cts.CancelAsync();
                return await Task.FromCanceled<Confluent.Kafka.DeliveryResult<string, byte[]>>(call.ArgAt<CancellationToken>(2));
            });
        var sink = CreateSink(producer);

        await Should.ThrowAsync<OperationCanceledException>(
            async () => await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), cts.Token));
    }

    private static IProducer<string, byte[]> FailingProducer(Error error)
    {
        var producer = Substitute.For<IProducer<string, byte[]>>();
        producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, byte[]>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<Confluent.Kafka.DeliveryResult<string, byte[]>>(
                new ProduceException<string, byte[]>(error, new Confluent.Kafka.DeliveryResult<string, byte[]>())));
        return producer;
    }
}
