using System.Text.Json;
using System.Text.Json.Serialization;
using Wallaby.Abstractions;
using static Wallaby.Sinks.Http.UnitTests.SinkTestHelpers;

namespace Wallaby.Sinks.Http.UnitTests;

/// <summary>The JSON envelope the sink POSTs: structure, metadata, and document value encoding.</summary>
public class EnvelopeTests
{
    /// <summary>Deliver one batch and parse the single captured request body.</summary>
    private static async Task<JsonDocument> CaptureEnvelopeAsync(SinkBatch batch, Action<HttpSinkOptions>? configure = null)
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler, configure);
        (await sink.DeliverAsync(batch, CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);
        return JsonDocument.Parse(handler.Requests.ShouldHaveSingleItem().Body);
    }

    [Test]
    public async Task Envelope_carries_upserts_and_deletes_with_metadata()
    {
        var timestamp = new DateTimeOffset(2026, 7, 6, 1, 2, 3, TimeSpan.Zero);
        var envelope = await CaptureEnvelopeAsync(Batch(
            Upsert("42", new Dictionary<string, object?> { ["name"] = "Kangaroo", ["price"] = 19.95m },
                metadata: Meta(commitIdx: 3, timestamp: timestamp, lsn: 271828)),
            Delete("43")));

        var root = envelope.RootElement;
        root.GetProperty("sink").GetString().ShouldBe(SinkName);
        root.GetProperty("sentAt").GetDateTimeOffset().ShouldBeInRange(
            DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(1));
        root.TryGetProperty("annotations", out _).ShouldBeFalse();

        var records = root.GetProperty("records").EnumerateArray().ToList();
        records.Count.ShouldBe(2);

        var upsert = records[0];
        upsert.GetProperty("operation").GetString().ShouldBe("upsert");
        upsert.GetProperty("id").GetString().ShouldBe("42");
        upsert.GetProperty("idempotencyKey").GetString().ShouldBe("271828:3:products:42");
        upsert.GetProperty("destination").GetString().ShouldBe("products");
        upsert.GetProperty("document").GetProperty("name").GetString().ShouldBe("Kangaroo");
        upsert.GetProperty("document").GetProperty("price").GetDecimal().ShouldBe(19.95m);

        var metadata = upsert.GetProperty("metadata");
        metadata.GetProperty("schema").GetString().ShouldBe("public");
        metadata.GetProperty("table").GetString().ShouldBe("products");
        metadata.GetProperty("action").GetString().ShouldBe("insert");
        metadata.GetProperty("commitLsn").GetString().ShouldBe("271828");
        metadata.GetProperty("commitIdx").GetInt32().ShouldBe(3);
        metadata.GetProperty("commitTimestamp").GetDateTimeOffset().ShouldBe(timestamp);
        metadata.GetProperty("isBackfill").GetBoolean().ShouldBeFalse();

        var delete = records[1];
        delete.GetProperty("operation").GetString().ShouldBe("delete");
        delete.GetProperty("id").GetString().ShouldBe("43");
        delete.GetProperty("idempotencyKey").GetString().ShouldBe("12345:0:products:43");
        delete.GetProperty("metadata").GetProperty("action").GetString().ShouldBe("delete");
        delete.TryGetProperty("document", out _).ShouldBeFalse();
    }

    [Test]
    public async Task Backfill_record_omits_the_commit_timestamp()
    {
        var envelope = await CaptureEnvelopeAsync(Batch(
            Upsert("1", new Dictionary<string, object?>(), metadata: Meta(backfill: true, lsn: 0, action: ChangeAction.Read))));

        var record = envelope.RootElement.GetProperty("records")[0];
        // Stable across backfill runs: keyed by row, not by (lsn, idx) — those are 0 for every backfill read.
        record.GetProperty("idempotencyKey").GetString().ShouldBe("backfill:products:1");

        var metadata = record.GetProperty("metadata");
        metadata.GetProperty("isBackfill").GetBoolean().ShouldBeTrue();
        metadata.GetProperty("action").GetString().ShouldBe("read");
        metadata.GetProperty("commitLsn").GetString().ShouldBe("0");
        metadata.TryGetProperty("commitTimestamp", out _).ShouldBeFalse();
    }

    [Test]
    public async Task Null_destination_is_written_as_null()
    {
        var envelope = await CaptureEnvelopeAsync(Batch(
            Upsert("1", new Dictionary<string, object?>(), destination: null)));

        var record = envelope.RootElement.GetProperty("records")[0];
        record.GetProperty("destination").ValueKind.ShouldBe(JsonValueKind.Null);
        // Without a destination, the key falls back to the qualified table for its scope.
        record.GetProperty("idempotencyKey").GetString().ShouldBe("12345:0:public.products:1");
    }

    [Test]
    public async Task Annotations_are_echoed_at_the_envelope_level()
    {
        var envelope = await CaptureEnvelopeAsync(
            Batch(Upsert("1", new Dictionary<string, object?>())),
            o => o.Annotations = new Dictionary<string, string> { ["env"] = "prod", ["region"] = "au" });

        var annotations = envelope.RootElement.GetProperty("annotations");
        annotations.GetProperty("env").GetString().ShouldBe("prod");
        annotations.GetProperty("region").GetString().ShouldBe("au");
    }

    [Test]
    public async Task Scalar_values_are_encoded_natively()
    {
        var envelope = await CaptureEnvelopeAsync(Batch(Upsert("1", new Dictionary<string, object?>
        {
            ["null"] = null,
            ["string"] = "text",
            ["bool"] = true,
            ["int"] = 7,
            ["long"] = 9_000_000_000L,
            ["double"] = 1.5d,
            ["decimal"] = 10.01m,
            ["guid"] = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            ["dateTimeOffset"] = new DateTimeOffset(2026, 7, 6, 1, 2, 3, TimeSpan.Zero),
            ["dateOnly"] = new DateOnly(2026, 7, 6),
            ["timeOnly"] = new TimeOnly(13, 30),
            ["timeSpan"] = TimeSpan.FromMinutes(90),
            ["char"] = 'x',
            ["bytes"] = new byte[] { 1, 2, 3 },
            ["uri"] = new Uri("https://example.com/a"),
            ["nested"] = new Dictionary<string, object?> { ["inner"] = 1 },
            ["array"] = new object?[] { 1, "two", null },
        })));

        var document = envelope.RootElement.GetProperty("records")[0].GetProperty("document");
        document.GetProperty("null").ValueKind.ShouldBe(JsonValueKind.Null);
        document.GetProperty("string").GetString().ShouldBe("text");
        document.GetProperty("bool").GetBoolean().ShouldBeTrue();
        document.GetProperty("int").GetInt32().ShouldBe(7);
        document.GetProperty("long").GetInt64().ShouldBe(9_000_000_000L);
        document.GetProperty("double").GetDouble().ShouldBe(1.5d);
        document.GetProperty("decimal").GetDecimal().ShouldBe(10.01m);
        document.GetProperty("guid").GetGuid().ShouldBe(Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
        document.GetProperty("dateTimeOffset").GetDateTimeOffset()
            .ShouldBe(new DateTimeOffset(2026, 7, 6, 1, 2, 3, TimeSpan.Zero));
        document.GetProperty("dateOnly").GetString().ShouldBe("2026-07-06");
        document.GetProperty("timeOnly").GetString().ShouldBe("13:30:00.0000000");
        document.GetProperty("timeSpan").GetString().ShouldBe("01:30:00");
        document.GetProperty("char").GetString().ShouldBe("x");
        document.GetProperty("bytes").GetBytesFromBase64().ShouldBe([1, 2, 3]);
        document.GetProperty("uri").GetString().ShouldBe("https://example.com/a");
        document.GetProperty("nested").GetProperty("inner").GetInt32().ShouldBe(1);
        document.GetProperty("array").EnumerateArray().Select(e => e.ToString()).ShouldBe(["1", "two", ""]);
    }

    [Test]
    public async Task Custom_type_is_serialized_through_the_configured_options()
    {
        var envelope = await CaptureEnvelopeAsync(
            Batch(Upsert("1", new Dictionary<string, object?> { ["money"] = new Money(12.5m, "AUD") })),
            o => o.SerializerOptions = new JsonSerializerOptions { TypeInfoResolver = EnvelopeTestsJsonContext.Default });

        var money = envelope.RootElement.GetProperty("records")[0].GetProperty("document").GetProperty("money");
        money.GetProperty("Amount").GetDecimal().ShouldBe(12.5m);
        money.GetProperty("Currency").GetString().ShouldBe("AUD");
    }

    [Test]
    public async Task Unserializable_value_fails_delivery_permanently()
    {
        var cyclic = new Node();
        cyclic.Self = cyclic;

        var handler = new CapturingHandler();
        var sink = CreateSink(handler);
        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?> { ["node"] = cyclic })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        handler.Requests.ShouldBeEmpty();
    }

    public sealed record Money(decimal Amount, string Currency);

    public sealed class Node
    {
        public Node? Self { get; set; }
    }
}

[JsonSerializable(typeof(EnvelopeTests.Money))]
internal sealed partial class EnvelopeTestsJsonContext : JsonSerializerContext;
