using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Routes change events using per-entity <see cref="EntityMapping"/>s. For each mapped entity type the
/// transform is invoked over the transaction's insert/update/read changes — <em>sub-grouped by scope key</em>
/// (e.g. tenant) so each invocation gets a same-scope <see cref="DbContext"/> and only that scope's changes —
/// producing a document per source key; a missing or null document becomes a deletion. Deletes are routed
/// directly by key (no transform), but still resolve their scope key so a scoped destination is honored.
/// One enrichment context is created per distinct scope key per batch and disposed at the end.
/// </summary>
internal sealed class MappingChangeRouter(
    IReadOnlyDictionary<Type, EntityMapping> mappings,
    IEnrichmentContextProvider contextProvider,
    WallabyInstrumentation? instrumentation = null) : IChangeRouter
{
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;
    private static readonly object NullScopeKey = new();
    private static readonly object SharedContextKey = new();

    public async ValueTask<IReadOnlyList<RoutedDocument>> RouteAsync(
        IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
    {
        var routed = new List<RoutedDocument>();
        var contexts = new Dictionary<object, DbContext>();
        try
        {
            foreach (var group in changes.GroupBy(c => c.EntityClrType))
            {
                if (!mappings.TryGetValue(group.Key, out var mapping))
                {
                    continue; // entity not mapped to any sink
                }

                var groupChanges = group.ToList();

                // Deletes: routed by key without the transform, but scoped destination still needs the key.
                foreach (var deletion in groupChanges.Where(c => c.Action == ChangeAction.Delete))
                {
                    var scopeKey = mapping.GetScopeKey(deletion);
                    routed.Add(Deletion(mapping, deletion, mapping.ResolveDestination(scopeKey)));
                }

                // De-duplicate non-delete changes by key (last wins within the batch).
                var byKey = new Dictionary<DocumentKey, ChangeEvent>();
                foreach (var change in groupChanges.Where(c => c.Action != ChangeAction.Delete))
                {
                    byKey[change.Key] = change;
                }

                if (byKey.Count == 0)
                {
                    continue;
                }

                foreach (var scopeGroup in byKey.Values.GroupBy(mapping.GetScopeKey))
                {
                    var scopeKey = scopeGroup.Key;
                    var subset = scopeGroup.ToList();
                    var destination = mapping.ResolveDestination(scopeKey);
                    var db = GetOrCreateContext(contexts, scopeKey);
                    var entityName = mapping.EntityClrType.Name;

                    using var activity = _instr.StartTransform();
                    if (activity is not null)
                    {
                        activity.SetTag(WallabyInstrumentation.EntityTag, entityName);
                        activity.SetTag("wallaby.batch.size", subset.Count);
                    }

                    var transformStart = WallabyInstrumentation.StartTimer();
                    var documents = await mapping.Transform.InvokeAsync(db, subset, ct);
                    _instr.RecordTransformDuration(entityName, transformStart);

                    foreach (var change in subset)
                    {
                        if (documents.TryGetValue(change.Key, out var document) && document is not null)
                        {
                            routed.Add(Upsert(mapping, change, document, destination));
                        }
                        else
                        {
                            // Omitted from the transform output (or mapped to null) => delete it from the sink.
                            routed.Add(Deletion(mapping, change, destination));
                        }
                    }
                }
            }
        }
        finally
        {
            foreach (var db in contexts.Values)
            {
                await db.DisposeAsync();
            }
        }

        return routed;
    }

    private DbContext GetOrCreateContext(Dictionary<object, DbContext> cache, object? scopeKey)
    {
        // Unscoped providers share one context per batch; scoped providers cache one per distinct key.
        var cacheKey = contextProvider.IsScoped ? scopeKey ?? NullScopeKey : SharedContextKey;
        if (!cache.TryGetValue(cacheKey, out var db))
        {
            db = contextProvider.Create(scopeKey);
            cache[cacheKey] = db;
        }
        return db;
    }

    private static RoutedDocument Upsert(EntityMapping mapping, ChangeEvent change, object document, string? destination)
        => new(mapping.SinkName, new SinkRecord(
            destination, mapping.GetDocumentId(change), document, IsDeletion: false, change.Metadata));

    private static RoutedDocument Deletion(EntityMapping mapping, ChangeEvent change, string? destination)
        => new(mapping.SinkName, new SinkRecord(
            destination, mapping.GetDocumentId(change), Document: null, IsDeletion: true, change.Metadata));
}
