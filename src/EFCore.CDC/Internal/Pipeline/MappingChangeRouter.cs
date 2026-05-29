using EFCore.CDC.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace EFCore.CDC.Internal.Pipeline;

/// <summary>
/// Routes change events using per-entity <see cref="EntityMapping"/>s. For each mapped entity type the
/// transform is invoked once over the transaction's insert/update/read changes (batched), producing a
/// document per source key; a missing or null document becomes a deletion. Deletes are routed directly by
/// key without invoking the transform. A single scoped <see cref="DbContext"/> is created per batch.
/// </summary>
internal sealed class MappingChangeRouter(
    IReadOnlyDictionary<Type, EntityMapping> mappings,
    Func<CancellationToken, ValueTask<DbContext>> dbContextFactory) : IChangeRouter
{
    public async ValueTask<IReadOnlyList<RoutedDocument>> RouteAsync(
        IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
    {
        var routed = new List<RoutedDocument>();
        DbContext? db = null;
        try
        {
            foreach (var group in changes.GroupBy(c => c.EntityClrType))
            {
                if (!mappings.TryGetValue(group.Key, out var mapping))
                {
                    continue; // entity not mapped to any sink
                }

                var groupChanges = group.ToList();

                foreach (var deletion in groupChanges.Where(c => c.Action == ChangeAction.Delete))
                {
                    routed.Add(Deletion(mapping, deletion));
                }

                // De-duplicate non-delete changes by key (last wins within the batch).
                var byKey = new Dictionary<DocumentKey, ChangeEvent>();
                foreach (var change in groupChanges.Where(c => c.Action != ChangeAction.Delete))
                {
                    byKey[new DocumentKey(change.PrimaryKey)] = change;
                }

                if (byKey.Count == 0)
                {
                    continue;
                }

                db ??= await dbContextFactory(ct);
                var documents = await mapping.Transform.InvokeAsync(db, byKey.Values.ToList(), ct);

                foreach (var (key, change) in byKey)
                {
                    if (documents.TryGetValue(key, out var document) && document is not null)
                    {
                        routed.Add(Upsert(mapping, change, document));
                    }
                    else
                    {
                        // Omitted from the transform output (or mapped to null) => delete it from the sink.
                        routed.Add(Deletion(mapping, change));
                    }
                }
            }
        }
        finally
        {
            if (db is not null)
            {
                await db.DisposeAsync();
            }
        }

        return routed;
    }

    private static RoutedDocument Upsert(EntityMapping mapping, ChangeEvent change, object document)
        => new(mapping.SinkName, new SinkRecord(
            mapping.Destination, mapping.GetDocumentId(change), document, IsDeletion: false, change.Metadata));

    private static RoutedDocument Deletion(EntityMapping mapping, ChangeEvent change)
        => new(mapping.SinkName, new SinkRecord(
            mapping.Destination, mapping.GetDocumentId(change), Document: null, IsDeletion: true, change.Metadata));
}
