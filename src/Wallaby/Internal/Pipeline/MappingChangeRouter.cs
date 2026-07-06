using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Providers;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Routes change events using per-entity <see cref="EntityMapping"/>s. An entity type may carry several
/// mappings (at most one per sink); each runs its own transform over the transaction's insert/update/read
/// changes — <em>sub-grouped by scope key</em> (e.g. tenant) so each invocation gets a same-scope enrichment
/// session and only that scope's changes — producing a document per source key; a missing or null document
/// becomes a deletion. Deletes are routed directly by key (no transform), but still resolve their scope key
/// so a scoped destination is honored. Sessions come from each mapping's <see cref="EntityMapping.Sessions"/>:
/// one lease per distinct (session provider, scope key) per batch — a type's mappings share a provider and so
/// share a session, while mappings on different providers lease independently — all disposed at the end.
/// </summary>
internal sealed class MappingChangeRouter : IChangeRouter
{
    private readonly Dictionary<Type, EntityMapping[]> _mappings;
    private readonly WallabyInstrumentation _instr;
    private static readonly object NullScopeKey = new();
    private static readonly object SharedContextKey = new();

    public MappingChangeRouter(IReadOnlyList<EntityMapping> mappings, WallabyInstrumentation? instrumentation = null)
    {
        // Declaration order within a type is preserved, so emission order is deterministic.
        _mappings = mappings.GroupBy(m => m.EntityClrType).ToDictionary(g => g.Key, g => g.ToArray());
        _instr = instrumentation ?? WallabyInstrumentation.NoOp;
    }

    public async ValueTask<IReadOnlyList<RoutedDocument>> RouteAsync(
        IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
    {
        var routed = new List<RoutedDocument>();
        var sessions = new Dictionary<(IEnrichmentSessionProvider, object), IEnrichmentSession>();
        try
        {
            foreach (var group in changes.GroupBy(c => c.EntityClrType))
            {
                if (!_mappings.TryGetValue(group.Key, out var typeMappings))
                {
                    continue; // entity not mapped to any sink
                }

                // Collapse to the last change per key in commit order. A key inserted/updated and then
                // deleted (or deleted and then re-inserted) within the same batch must resolve to its
                // FINAL action — exactly one routed record per mapping, never both an upsert and a
                // deletion. (GroupBy yields each group's elements in source order, and the batch is in
                // commit order.)
                var lastByKey = new Dictionary<DocumentKey, ChangeEvent>();
                foreach (var change in group)
                {
                    lastByKey[change.Key] = change;
                }

                // Split the collapsed changes once — the final action is mapping-independent. A key whose
                // final action is a delete is routed directly by key without a transform (the row is gone —
                // a scoped destination still resolves the key from the old row); the rest go through each
                // mapping's transform.
                List<ChangeEvent>? deletes = null;
                List<ChangeEvent>? upserts = null;
                foreach (var change in lastByKey.Values)
                {
                    if (change.Action == ChangeAction.Delete)
                    {
                        (deletes ??= []).Add(change);
                    }
                    else
                    {
                        (upserts ??= []).Add(change);
                    }
                }

                foreach (var mapping in typeMappings)
                {
                    if (deletes is not null)
                    {
                        foreach (var change in deletes)
                        {
                            var scopeKey = mapping.GetScopeKey(change);
                            routed.Add(Deletion(mapping, change, mapping.ResolveDestination(scopeKey)));
                        }
                    }

                    if (upserts is null)
                    {
                        continue;
                    }

                    foreach (var scopeGroup in upserts.GroupBy(mapping.GetScopeKey))
                    {
                        var scopeKey = scopeGroup.Key;
                        var subset = scopeGroup.ToList();
                        var destination = mapping.ResolveDestination(scopeKey);
                        var session = GetOrCreateSession(sessions, mapping.Sessions, scopeKey);
                        var entityName = mapping.EntityClrType.Name;

                        using var activity = _instr.StartTransform();
                        if (activity is not null)
                        {
                            activity.SetTag(WallabyInstrumentation.EntityTag, entityName);
                            activity.SetTag(WallabyInstrumentation.SinkTag, mapping.SinkName);
                            activity.SetTag("wallaby.batch.size", subset.Count);
                        }

                        var transformStart = WallabyInstrumentation.StartTimer();
                        // A transform exception always propagates and halts the pipeline
                        var documents = await mapping.Transform.InvokeAsync(session, subset, ct);
                        _instr.RecordTransformDuration(entityName, mapping.SinkName, transformStart);

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
        }
        finally
        {
            foreach (var lease in sessions.Values)
            {
                await lease.DisposeAsync();
            }
        }

        return routed;
    }

    private static object GetOrCreateSession(
        Dictionary<(IEnrichmentSessionProvider, object), IEnrichmentSession> cache,
        IEnrichmentSessionProvider sessionProvider, object? scopeKey)
    {
        // Unscoped providers share one session per batch; scoped providers cache one per distinct key.
        // Keyed by the session provider too, so mappings on different storage providers lease independently.
        var cacheKey = (sessionProvider, sessionProvider.IsScoped ? scopeKey ?? NullScopeKey : SharedContextKey);
        if (!cache.TryGetValue(cacheKey, out var lease))
        {
            lease = sessionProvider.Lease(scopeKey);
            cache[cacheKey] = lease;
        }
        return lease.Session;
    }

    private static RoutedDocument Upsert(
        EntityMapping mapping, ChangeEvent change, IReadOnlyDictionary<string, object?> document, string? destination)
        => new(mapping.SinkName, new SinkRecord(
            destination, mapping.GetDocumentId(change), document, IsDeletion: false, change.Metadata));

    private static RoutedDocument Deletion(EntityMapping mapping, ChangeEvent change, string? destination)
        => new(mapping.SinkName, new SinkRecord(
            destination, mapping.GetDocumentId(change), Document: null, IsDeletion: true, change.Metadata));
}
