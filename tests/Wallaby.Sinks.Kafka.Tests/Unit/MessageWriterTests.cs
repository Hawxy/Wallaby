using System.Text;
using System.Text.Json;
using Wallaby.Sinks.Kafka.Internal;
using static Wallaby.Sinks.Kafka.Tests.Unit.KafkaTestHelpers;

namespace Wallaby.Sinks.Kafka.Tests.Unit;

/// <summary>Shape of the per-message JSON envelope and headers.</summary>
public class MessageWriterTests
{
    [Test]
    public void Upsert_envelope_is_self_contained()
    {
        var record = Upsert("42", new Dictionary<string, object?>
        {
            ["name"] = "alpha",
            ["price"] = 9.5,
            ["tags"] = new[] { "a", "b" },
            ["nested"] = new Dictionary<string, object?> { ["x"] = 1 },
        }, metadata: Meta(commitIdx: 3, timestamp: DateTimeOffset.UnixEpoch));

        using var envelope = JsonDocument.Parse(KafkaMessageWriter.WriteValue(record, annotations: null, serializerOptions: null));

        var root = envelope.RootElement;
        root.GetProperty("operation").GetString().ShouldBe("upsert");
        root.GetProperty("id").GetString().ShouldBe("42");
        root.GetProperty("idempotencyKey").GetString().ShouldBe("12345:3:products:42");
        var document = root.GetProperty("document");
        document.GetProperty("name").GetString().ShouldBe("alpha");
        document.GetProperty("price").GetDouble().ShouldBe(9.5);
        document.GetProperty("tags").GetArrayLength().ShouldBe(2);
        document.GetProperty("nested").GetProperty("x").GetInt32().ShouldBe(1);
        var metadata = root.GetProperty("metadata");
        metadata.GetProperty("schema").GetString().ShouldBe("public");
        metadata.GetProperty("table").GetString().ShouldBe("products");
        metadata.GetProperty("action").GetString().ShouldBe("insert");
        metadata.GetProperty("commitLsn").GetString().ShouldBe("12345"); // string: ulong exceeds JS safe integers
        metadata.GetProperty("commitIdx").GetInt32().ShouldBe(3);
        metadata.GetProperty("commitTimestamp").GetDateTimeOffset().ShouldBe(DateTimeOffset.UnixEpoch);
        metadata.GetProperty("isBackfill").GetBoolean().ShouldBeFalse();
    }

    [Test]
    public void Annotations_are_echoed()
    {
        var value = KafkaMessageWriter.WriteValue(
            Upsert("1", new Dictionary<string, object?> { ["name"] = "a" }),
            annotations: new Dictionary<string, string> { ["env"] = "prod" },
            serializerOptions: null);

        using var envelope = JsonDocument.Parse(value);
        envelope.RootElement.GetProperty("annotations").GetProperty("env").GetString().ShouldBe("prod");
    }

    [Test]
    public void Backfill_records_share_an_idempotency_key_across_runs()
    {
        var first = Upsert("7", new Dictionary<string, object?>(), metadata: Meta(backfill: true, lsn: 0));
        var second = Upsert("7", new Dictionary<string, object?>(), metadata: Meta(backfill: true, lsn: 0, commitIdx: 9));

        KafkaMessageWriter.IdempotencyKey(first).ShouldBe("backfill:products:7");
        KafkaMessageWriter.IdempotencyKey(second).ShouldBe("backfill:products:7");
    }

    [Test]
    public void Live_idempotency_keys_are_unique_per_change()
    {
        KafkaMessageWriter.IdempotencyKey(Upsert("7", new Dictionary<string, object?>(), metadata: Meta(commitIdx: 0)))
            .ShouldNotBe(KafkaMessageWriter.IdempotencyKey(Upsert("7", new Dictionary<string, object?>(), metadata: Meta(commitIdx: 1))));
    }

    [Test]
    public void Headers_carry_the_tombstones_only_context()
    {
        var headers = KafkaMessageWriter.BuildHeaders(Delete("42"));

        Header(headers, KafkaMessageWriter.OperationHeader).ShouldBe("delete");
        Header(headers, KafkaMessageWriter.IdempotencyKeyHeader).ShouldBe("12345:0:products:42");
        Header(headers, KafkaMessageWriter.TableHeader).ShouldBe("public.products");
        Header(headers, KafkaMessageWriter.CommitLsnHeader).ShouldBe("12345");
    }

    private static string Header(Confluent.Kafka.Headers headers, string key) =>
        Encoding.UTF8.GetString(headers.GetLastBytes(key));
}
