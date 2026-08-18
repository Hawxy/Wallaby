using System.Text.Json;
using Dekaf.Errors;
using Dekaf.Producer;
using Dekaf.Protocol;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
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
        produced.Select(p => (p.Topic, p.Key)).ShouldBe([("products", "1"), ("other", "2"), ("products", "1")]);
    }

    [Test]
    public async Task Deletions_are_tombstones()
    {
        var (sink, produced) = CreateSink();

        await sink.DeliverAsync(Batch(Delete("42")), CancellationToken.None);

        var message = produced.ShouldHaveSingleItem();
        message.Value.ShouldBeNull();
        message.Headers.ShouldNotBeNull().GetFirstAsString(KafkaMessageWriter.OperationHeader).ShouldBe("delete");
    }

    [Test]
    public async Task Upsert_values_are_the_json_envelope()
    {
        var (sink, produced) = CreateSink();

        await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?> { ["name"] = "a" })), CancellationToken.None);

        using var envelope = JsonDocument.Parse(produced.Single().Value);
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
    [Arguments(ErrorCode.MessageTooLarge, DeliveryStatus.PermanentFailure)]
    [Arguments(ErrorCode.TopicAuthorizationFailed, DeliveryStatus.PermanentFailure)]
    [Arguments(ErrorCode.SaslAuthenticationFailed, DeliveryStatus.PermanentFailure)]
    [Arguments(ErrorCode.InvalidRecord, DeliveryStatus.PermanentFailure)]
    [Arguments(ErrorCode.OutOfOrderSequenceNumber, DeliveryStatus.PermanentFailure)]
    [Arguments(ErrorCode.UnknownTopicOrPartition, DeliveryStatus.RetryableFailure)]
    [Arguments(ErrorCode.RequestTimedOut, DeliveryStatus.RetryableFailure)]
    [Arguments(ErrorCode.NetworkException, DeliveryStatus.RetryableFailure)]
    [Arguments(ErrorCode.NotLeaderOrFollower, DeliveryStatus.RetryableFailure)]
    public async Task Broker_errors_are_classified(ErrorCode code, DeliveryStatus expected)
    {
        var sink = CreateSink(FailingProducer(new ProduceException(code, $"broker rejected: {code}")));

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(expected);
        result.Error.ShouldNotBeNull().ShouldContain(code.ToString());
    }

    [Test]
    public async Task Delivery_timeouts_are_retryable()
    {
        // KafkaTimeoutException carries no protocol error code; it must still classify as retryable.
        var sink = CreateSink(FailingProducer(new KafkaTimeoutException("delivery timed out")));

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error.ShouldNotBeNull().ShouldContain("timed out");
    }

    [Test]
    public async Task One_failed_report_fails_the_batch_after_all_reports_settle()
    {
        var settled = 0;
        var calls = 0;
        var producer = Substitute.For<IKafkaProducer<string, byte[]>>();
        producer.ProduceAsync(Arg.Any<ProducerMessage<string, byte[]>>(), Arg.Any<CancellationToken>())
            .Returns(_ => Interlocked.Increment(ref calls) == 1
                ? ValueTask.FromException<RecordMetadata>(new KafkaTimeoutException("delivery timed out"))
                : SlowSuccess());
        var sink = CreateSink(producer);

        async ValueTask<RecordMetadata> SlowSuccess()
        {
            await Task.Delay(50);
            Interlocked.Increment(ref settled);
            return new RecordMetadata
            {
                Topic = "products",
                Partition = 0,
                Offset = 0,
                Timestamp = DateTimeOffset.UnixEpoch,
            };
        }

        var result = await sink.DeliverAsync(
            Batch(
                Upsert("1", new Dictionary<string, object?>()),
                Upsert("2", new Dictionary<string, object?>())),
            CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        settled.ShouldBe(1); // the slower in-flight report was still awaited before failing the batch
    }

    [Test]
    public async Task A_synchronously_thrown_produce_exception_is_classified_not_escaped()
    {
        // ProduceAsync can throw (rather than returning a faulted task) when the local buffer is full.
        var producer = Substitute.For<IKafkaProducer<string, byte[]>>();
        producer.ProduceAsync(Arg.Any<ProducerMessage<string, byte[]>>(), Arg.Any<CancellationToken>())
            .Throws(new KafkaTimeoutException("buffer full"));
        var sink = CreateSink(producer);

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error.ShouldNotBeNull().ShouldContain("buffer full");
    }

    [Test]
    public async Task A_batch_that_fails_validation_produces_nothing()
    {
        var (sink, produced) = CreateSink();

        var result = await sink.DeliverAsync(
            Batch(
                Upsert("1", new Dictionary<string, object?>()),
                Upsert("2", new Dictionary<string, object?>(), destination: null)),
            CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        produced.ShouldBeEmpty(); // the routable first record was not produced either
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
        var producer = Substitute.For<IKafkaProducer<string, byte[]>>();
        producer.ProduceAsync(Arg.Any<ProducerMessage<string, byte[]>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                cts.Cancel();
                return new ValueTask<RecordMetadata>(Task.FromCanceled<RecordMetadata>(call.ArgAt<CancellationToken>(1)));
            });
        var sink = CreateSink(producer);

        await Should.ThrowAsync<OperationCanceledException>(
            async () => await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), cts.Token));
    }

    private static IKafkaProducer<string, byte[]> FailingProducer(KafkaException exception)
    {
        var producer = Substitute.For<IKafkaProducer<string, byte[]>>();
        producer.ProduceAsync(Arg.Any<ProducerMessage<string, byte[]>>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromException<RecordMetadata>(exception));
        return producer;
    }
}
