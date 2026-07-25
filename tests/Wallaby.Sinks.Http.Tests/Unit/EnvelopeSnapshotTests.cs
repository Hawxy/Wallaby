using System.Text;
using System.Text.Json;
using Wallaby.Abstractions;
using static Wallaby.Sinks.Http.Tests.Unit.SinkTestHelpers;

namespace Wallaby.Sinks.Http.Tests.Unit;

/// <summary>Pins the exact HTTP body as a Verify snapshot, so any wire-format change surfaces as a diff.</summary>
public class EnvelopeSnapshotTests
{
    [Test]
    public async Task Http_body_matches_the_approved_envelope()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler, o => o.Annotations = new Dictionary<string, string> { ["env"] = "test" });

        var timestamp = new DateTimeOffset(2026, 7, 6, 1, 2, 3, 123, TimeSpan.Zero);
        await sink.DeliverAsync(Batch(
            Upsert("42", new Dictionary<string, object?>
            {
                ["name"] = "Kangaroo plush",
                ["price"] = 19.95m,
                ["tags"] = new object?[] { "toys", null },
                ["dimensions"] = new Dictionary<string, object?> { ["heightCm"] = 30 },
            }, metadata: Meta(commitIdx: 0, timestamp: timestamp, lsn: 27271208)),
            Delete("43", Meta(commitIdx: 1, timestamp: timestamp, lsn: 27271208, action: ChangeAction.Delete)),
            Upsert("7", new Dictionary<string, object?> { ["name"] = "Backfilled" },
                metadata: Meta(backfill: true, lsn: 0, action: ChangeAction.Read,
                    backfillRunId: "3f2c8a41d96e4f0f9c2b7e5a1d6b8c90"))), CancellationToken.None);

        var body = Encoding.UTF8.GetString(handler.Requests.ShouldHaveSingleItem().Body);

        // Indented via System.Text.Json (not Verify's relaxed JSON) so quoting and value types are
        // pinned exactly as they appear on the wire. Only sentAt is nondeterministic.
        using var parsed = JsonDocument.Parse(body);
        var pretty = JsonSerializer.Serialize(parsed, new JsonSerializerOptions { WriteIndented = true });

        await Verify(pretty, extension: "json")
            .ScrubLinesWithReplace(line => line.Contains("\"sentAt\"") ? "  \"sentAt\": \"{Scrubbed}\"," : line);
    }
}
