using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;

namespace Wallaby.TestInfrastructure;

/// <summary>
/// A router that sends every change to every registered sink, using the change's primary key as the
/// document id and the change's current values (<see cref="ChangeEvent.Record"/>) as the document
/// payload — no mappings or transforms. Backs the harness's <see cref="WallabyTestHarness.Broadcast"/>
/// mode, which lets pipeline tests observe raw captured rows.
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
                    Document: isDeletion ? null : change.Record,
                    IsDeletion: isDeletion,
                    Metadata: change.Metadata)));
            }
        }

        return ValueTask.FromResult<IReadOnlyList<RoutedDocument>>(routed);
    }
}
