using System.Text;
using System.Text.Json;
using Wallaby.Sinks.OpenSearch.Internal;
using static Wallaby.Sinks.OpenSearch.Tests.Unit.SinkTestHelpers;

namespace Wallaby.Sinks.OpenSearch.Tests.Unit;

/// <summary>NDJSON shape of the <c>_bulk</c> bodies produced by <see cref="BulkWriter"/>.</summary>
public class BulkPayloadTests
{
    private static string[] Lines(byte[] payload)
    {
        var text = Encoding.UTF8.GetString(payload);
        text.ShouldEndWith("\n"); // the _bulk API requires a trailing newline
        return text[..^1].Split('\n');
    }

    private static byte[] Write(IReadOnlyList<Wallaby.Abstractions.SinkRecord> records,
        int offset = 0, int? count = null, string? defaultIndex = null)
        => BulkWriter.Write(SinkName, records, offset, count ?? records.Count, defaultIndex, serializerOptions: null);

    [Test]
    public void Upsert_writes_an_action_line_and_a_document_line()
    {
        var payload = Write([Upsert("1", new Dictionary<string, object?> { ["name"] = "alpha" })]);

        var lines = Lines(payload);
        lines.Length.ShouldBe(2);

        using var action = JsonDocument.Parse(lines[0]);
        var index = action.RootElement.GetProperty("index");
        index.GetProperty("_index").GetString().ShouldBe("products");
        index.GetProperty("_id").GetString().ShouldBe("1");

        using var document = JsonDocument.Parse(lines[1]);
        document.RootElement.GetProperty("name").GetString().ShouldBe("alpha");
    }

    [Test]
    public void Delete_writes_only_an_action_line()
    {
        var payload = Write([Delete("9")]);

        var lines = Lines(payload);
        lines.Length.ShouldBe(1);

        using var action = JsonDocument.Parse(lines[0]);
        var delete = action.RootElement.GetProperty("delete");
        delete.GetProperty("_index").GetString().ShouldBe("products");
        delete.GetProperty("_id").GetString().ShouldBe("9");
    }

    [Test]
    public void Destination_falls_back_to_the_default_index()
    {
        var payload = Write([Upsert("1", new Dictionary<string, object?>(), destination: null)], defaultIndex: "fallback");

        using var action = JsonDocument.Parse(Lines(payload)[0]);
        action.RootElement.GetProperty("index").GetProperty("_index").GetString().ShouldBe("fallback");
    }

    [Test]
    public void Record_without_destination_or_default_index_throws()
    {
        var ex = Should.Throw<InvalidOperationException>(
            () => Write([Upsert("1", new Dictionary<string, object?>(), destination: null)]));
        ex.Message.ShouldContain(SinkName);
        ex.Message.ShouldContain("DefaultIndex");
    }

    [Test]
    public void Only_the_requested_slice_is_written()
    {
        var records = new[]
        {
            Upsert("1", new Dictionary<string, object?>()),
            Upsert("2", new Dictionary<string, object?>()),
            Upsert("3", new Dictionary<string, object?>()),
        };

        var payload = Write(records, offset: 1, count: 1);

        var lines = Lines(payload);
        lines.Length.ShouldBe(2);
        using var action = JsonDocument.Parse(lines[0]);
        action.RootElement.GetProperty("index").GetProperty("_id").GetString().ShouldBe("2");
    }

    [Test]
    public void Scalar_document_values_are_written_natively()
    {
        var document = new Dictionary<string, object?>
        {
            ["s"] = "text",
            ["b"] = true,
            ["i"] = 42,
            ["l"] = 42L,
            ["d"] = 1.5,
            ["m"] = 9.99m,
            ["guid"] = Guid.Parse("f2f4f2f4-0000-0000-0000-000000000001"),
            ["date"] = new DateOnly(2026, 7, 8),
            ["nested"] = new Dictionary<string, object?> { ["x"] = 1 },
            ["seq"] = new[] { "a", "b" },
            ["nil"] = null,
        };

        var payload = Write([Upsert("1", document)]);

        using var parsed = JsonDocument.Parse(Lines(payload)[1]);
        var root = parsed.RootElement;
        root.GetProperty("s").GetString().ShouldBe("text");
        root.GetProperty("b").GetBoolean().ShouldBeTrue();
        root.GetProperty("i").GetInt32().ShouldBe(42);
        root.GetProperty("l").GetInt64().ShouldBe(42L);
        root.GetProperty("d").GetDouble().ShouldBe(1.5);
        root.GetProperty("m").GetDecimal().ShouldBe(9.99m);
        root.GetProperty("guid").GetGuid().ShouldBe(Guid.Parse("f2f4f2f4-0000-0000-0000-000000000001"));
        root.GetProperty("date").GetString().ShouldBe("2026-07-08");
        root.GetProperty("nested").GetProperty("x").GetInt32().ShouldBe(1);
        root.GetProperty("seq").EnumerateArray().Select(e => e.GetString()).ShouldBe(["a", "b"]);
        root.GetProperty("nil").ValueKind.ShouldBe(JsonValueKind.Null);
    }

    [Test]
    public void Mixed_upserts_and_deletes_preserve_record_order()
    {
        var payload = Write(
        [
            Upsert("1", new Dictionary<string, object?> { ["name"] = "alpha" }),
            Delete("2"),
            Upsert("3", new Dictionary<string, object?> { ["name"] = "gamma" }),
        ]);

        var lines = Lines(payload);
        lines.Length.ShouldBe(5); // action+doc, action, action+doc
        lines[0].ShouldContain("\"index\"");
        lines[2].ShouldContain("\"delete\"");
        lines[3].ShouldContain("\"index\"");
    }
}
