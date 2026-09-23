using System.Runtime.InteropServices;
using NpgsqlTypes;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Providers;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Routes change events using per-entity <see cref="EntityMapping"/>s. An entity type may carry several
/// mappings (at most one per sink). Each entity's changes collapse to the last change per source key; each
/// mapping then runs its transform over the non-delete changes, <em>sub-grouped by scope key</em> (e.g. tenant)
/// so each invocation gets a same-scope enrichment session and only that scope's changes. A missing or null
/// document becomes a deletion. Deletes are routed directly by key (no transform), but still resolve their
/// scope key so a scoped destination is honored. Records are emitted in commit order, one per
/// (destination, document id), the latest change winning.
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
        await using var sessions = new SessionLeases();
        var routed = new List<RoutedDocument>();
        foreach (var (type, group) in OrderedGrouping.GroupPreservingOrder(changes, c => c.EntityClrType))
        {
            if (!_mappings.TryGetValue(type, out var mappings))
            {
                continue; // entity not mapped to any sink
            }

            // A key changed several times in the batch (e.g. inserted then deleted) resolves to its final action.
            var final = KeepLast(group, c => c.Key);
            foreach (var mapping in mappings)
            {
                var upserts = await TransformAsync(mapping, final, sessions, ct);
                var records = final.ConvertAll(change => ToRecord(mapping, change, upserts));

                // A custom document id can be shared by several source rows.
                routed.AddRange(KeepLast(records, r => (r.Record.Destination, r.Record.DocumentId)));
            }
        }
        return routed;
    }

    /// <summary>
    /// Runs the mapping's transform over the non-delete changes, once per scope key, returning each change's
    /// destination and document (null when the transform omitted it or mapped it to null).
    /// </summary>
    private async Task<Dictionary<DocumentKey, (string? Destination, WallabyDocument? Document)>> TransformAsync(
        EntityMapping mapping, List<ChangeEvent> final, SessionLeases sessions, CancellationToken ct)
    {
        var byScope = new Dictionary<object, (object? ScopeKey, List<ChangeEvent> Changes)>();
        foreach (var change in final)
        {
            if (change.Action == ChangeAction.Delete)
            {
                continue;
            }

            var scopeKey = mapping.GetScopeKey(change);
            ref var scope = ref CollectionsMarshal.GetValueRefOrAddDefault(byScope, scopeKey ?? NullScopeKey, out var exists);
            if (!exists)
            {
                scope = (scopeKey, []);
            }
            scope.Changes.Add(change);
        }

        var results = new Dictionary<DocumentKey, (string?, WallabyDocument?)>();
        foreach (var (scopeKey, changes) in byScope.Values)
        {
            var destination = mapping.ResolveDestination(scopeKey);
            var session = sessions.Get(mapping.Sessions, scopeKey);
            var documents = await InvokeTransformAsync(mapping, session, changes, destination, ct);
            foreach (var change in changes)
            {
                results[change.Key] = (destination, documents.GetValueOrDefault(change.Key));
            }
        }
        return results;
    }

    private async Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeTransformAsync(
        EntityMapping mapping, object session, List<ChangeEvent> changes, string? destination, CancellationToken ct)
    {
        var entityName = mapping.EntityClrType.Name;
        using var activity = _instr.StartTransform();
        if (activity is not null)
        {
            activity.SetTag(WallabyInstrumentation.EntityTag, entityName);
            activity.SetTag(WallabyInstrumentation.SinkTag, mapping.SinkName);
            activity.SetTag(WallabyInstrumentation.DestinationTag, destination);
            activity.SetTag("wallaby.batch.size", changes.Count);
        }

        var transformStart = WallabyInstrumentation.StartTimer();
        IReadOnlyDictionary<DocumentKey, WallabyDocument?> documents;
        try
        {
            // A transform exception always propagates and halts the pipeline.
            documents = await mapping.Transform.InvokeAsync(session, changes, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"Transform for {entityName} (sink '{mapping.SinkName}', destination '{destination}') " +
                $"failed on a batch of {changes.Count} change(s) from {changes[0].Metadata.QualifiedTableName} " +
                $"starting at commit {new NpgsqlLogSequenceNumber(changes[0].Metadata.CommitLsn)}: {ex.Message}", ex);
        }
        _instr.RecordTransformDuration(entityName, mapping.SinkName, transformStart);
        return documents;
    }

    /// <summary>
    /// The record for one final change: an upsert of its transformed document, or a deletion when the change
    /// is a delete or the transform produced no document.
    /// </summary>
    private static RoutedDocument ToRecord(
        EntityMapping mapping, ChangeEvent change,
        Dictionary<DocumentKey, (string? Destination, WallabyDocument? Document)> upserts)
    {
        if (change.Action != ChangeAction.Delete)
        {
            var (destination, document) = upserts[change.Key];
            return Record(mapping, change, destination, document);
        }

        try
        {
            return Record(mapping, change, mapping.ResolveDestination(mapping.GetScopeKey(change)), document: null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"Routing a delete of {mapping.EntityClrType.Name} (sink '{mapping.SinkName}') " +
                $"from {change.Metadata.QualifiedTableName} failed at commit " +
                $"{new NpgsqlLogSequenceNumber(change.Metadata.CommitLsn)}: {ex.Message}", ex);
        }
    }

    private static RoutedDocument Record(
        EntityMapping mapping, ChangeEvent change, string? destination, WallabyDocument? document)
        => new(mapping.SinkName, new SinkRecord(
            destination, mapping.GetDocumentId(change), document, IsDeletion: document is null, change.Metadata));

    /// <summary>The last item per key, in source order.</summary>
    private static List<T> KeepLast<T, TKey>(IReadOnlyList<T> items, Func<T, TKey> keySelector)
        where TKey : notnull
    {
        var seen = new HashSet<TKey>(items.Count);
        var kept = new List<T>(items.Count);
        for (var i = items.Count - 1; i >= 0; i--)
        {
            if (seen.Add(keySelector(items[i])))
            {
                kept.Add(items[i]);
            }
        }
        kept.Reverse();
        return kept;
    }

    /// <summary>
    /// The enrichment session leases of one routing pass, all disposed when it ends. Unscoped providers share
    /// one session per batch; scoped providers lease one per distinct scope key. Keyed by the session provider
    /// too, so mappings on different storage providers lease independently.
    /// </summary>
    private sealed class SessionLeases : IAsyncDisposable
    {
        private readonly Dictionary<(IEnrichmentSessionProvider, object), IEnrichmentSession> _leases = [];

        public object Get(IEnrichmentSessionProvider provider, object? scopeKey)
        {
            var cacheKey = (provider, provider.IsScoped ? scopeKey ?? NullScopeKey : SharedContextKey);
            if (!_leases.TryGetValue(cacheKey, out var lease))
            {
                lease = provider.Lease(scopeKey);
                _leases[cacheKey] = lease;
            }
            return lease.Session;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var lease in _leases.Values)
            {
                await lease.DisposeAsync();
            }
        }
    }
}
