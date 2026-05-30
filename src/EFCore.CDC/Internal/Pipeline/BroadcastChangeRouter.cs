using EFCore.CDC.Abstractions;

namespace EFCore.CDC.Internal.Pipeline;

/// <summary>
/// A simple router that sends every change to every registered sink, using the change's primary key as
/// the document id and the <see cref="ChangeEvent"/> itself as the document payload. Used before
/// mapping/transform-based routing is configured (and as a default for sinks without a transform).
/// </summary>
internal sealed class BroadcastChangeRouter(IReadOnlyList<string> sinkNames) : IChangeRouter
{
    public ValueTask<IReadOnlyList<RoutedDocument>> RouteAsync(
        IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
    {
        var routed = new List<RoutedDocument>(changes.Count * sinkNames.Count);

        foreach (var change in changes)
        {
            var documentId = change.Key.ToString();
            var isDeletion = change.Action == ChangeAction.Delete;

            foreach (var sinkName in sinkNames)
            {
                routed.Add(new RoutedDocument(sinkName, new SinkRecord(
                    Destination: null,
                    DocumentId: documentId,
                    Document: isDeletion ? null : change,
                    IsDeletion: isDeletion,
                    Metadata: change.Metadata)));
            }
        }

        return ValueTask.FromResult<IReadOnlyList<RoutedDocument>>(routed);
    }
}
