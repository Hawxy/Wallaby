using EFCore.CDC.Abstractions;

namespace EFCore.CDC.Internal.Pipeline;

/// <summary>One transformed record bound to a specific sink.</summary>
internal sealed record RoutedDocument(string SinkName, SinkRecord Record);

/// <summary>
/// Maps a committed transaction's change events into per-sink records, preserving commit order. The M6
/// default broadcasts changes to all sinks; M7 introduces mapping/transform-based routing.
/// </summary>
internal interface IChangeRouter
{
    ValueTask<IReadOnlyList<RoutedDocument>> RouteAsync(IReadOnlyList<ChangeEvent> changes, CancellationToken ct);
}
