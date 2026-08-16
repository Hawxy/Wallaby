using Wallaby.Abstractions;

namespace Wallaby.Sinks.Elasticsearch.Tests.Unit;

internal static class SinkTestHelpers
{
    public const string SinkName = "search";

    public static ChangeMetadata Meta(ChangeAction action = ChangeAction.Insert)
        => new("public", "products", action, DateTimeOffset.UtcNow, 12345, 0, false);

    public static SinkRecord Upsert(string id, IReadOnlyDictionary<string, object?> document,
        string? destination = "products")
        => new(destination, id, document, false, Meta());

    public static SinkRecord Delete(string id, string? destination = "products")
        => new(destination, id, null, true, Meta(ChangeAction.Delete));

    public static SinkBatch Batch(params SinkRecord[] records) => new(SinkName, records);
}
