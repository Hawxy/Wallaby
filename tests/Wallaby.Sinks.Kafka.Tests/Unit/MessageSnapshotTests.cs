using System.Text.Json;
using Wallaby.Abstractions;
using Wallaby.Sinks.Kafka.Internal;
using static Wallaby.Sinks.Kafka.Tests.Unit.KafkaTestHelpers;

namespace Wallaby.Sinks.Kafka.Tests.Unit;

/// <summary>Pins the exact message value and headers as Verify snapshots, so any wire-format change surfaces as a diff.</summary>
public class MessageSnapshotTests
{
    [Test]
    public async Task Message_value_matches_the_approved_envelope()
    {
        var timestamp = new DateTimeOffset(2026, 7, 6, 1, 2, 3, 123, TimeSpan.Zero);
        var record = Upsert("42", new Dictionary<string, object?>
        {
            ["name"] = "Kangaroo plush",
            ["price"] = 19.95m,
            ["tags"] = new object?[] { "toys", null },
            ["dimensions"] = new Dictionary<string, object?> { ["heightCm"] = 30 },
        }, metadata: Meta(commitIdx: 0, timestamp: timestamp, lsn: 27271208));

        var value = KafkaMessageWriter.WriteValue(
            record, annotations: new Dictionary<string, string> { ["env"] = "test" }, serializerOptions: null);

        // Indented via System.Text.Json (not Verify's relaxed JSON) so quoting and value types are
        // pinned exactly as they appear on the wire.
        using var parsed = JsonDocument.Parse(value);
        var pretty = JsonSerializer.Serialize(parsed, new JsonSerializerOptions { WriteIndented = true });

        await Verify(pretty, extension: "json");
    }

    [Test]
    public async Task Tombstone_headers_match_the_approved_set()
    {
        var headers = KafkaMessageWriter.BuildHeaders(
            Delete("43", metadata: Meta(commitIdx: 1, lsn: 27271208, action: ChangeAction.Delete)));

        var lines = string.Join(Environment.NewLine,
            headers.Select(h => $"{h.Key}={h.GetValueAsString()}"));

        await Verify(lines);
    }
}
