using Wallaby.Abstractions;

namespace Wallaby.Internal.Pipeline;

/// <summary>One transformed record bound to a specific sink.</summary>
internal sealed record RoutedDocument(string SinkName, SinkRecord Record);

/// <summary>Maps a committed transaction's change events into per-sink records, preserving commit order.</summary>
internal interface IChangeRouter
{
    ValueTask<IReadOnlyList<RoutedDocument>> RouteAsync(IReadOnlyList<ChangeEvent> changes, CancellationToken ct);
}
