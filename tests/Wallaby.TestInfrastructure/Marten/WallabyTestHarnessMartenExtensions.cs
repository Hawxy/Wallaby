using Marten;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Providers.Marten;
using Wallaby.Providers.Marten.Internal;
using Wallaby.TestInfrastructure;

namespace Wallaby.TestInfrastructure.Marten;

/// <summary>
/// Marten test-store surface for <see cref="WallabyTestHarness"/>: the <c>ForMartenStore</c> factory plus
/// <c>Map</c>/<c>Project</c>/<c>UseTenantSessions</c> configuration against the shared test documents.
/// </summary>
public static class WallabyTestHarnessMartenExtensions
{
    extension(WallabyTestHarness)
    {
        /// <summary>Create a harness capturing <paramref name="store"/>'s documents and leasing its query sessions.</summary>
        public static WallabyTestHarness ForMartenStore(
            IDocumentStore store, string connectionString, WallabyNames? names = null)
            => new WallabyTestHarness(connectionString, new MartenModelProvider(store.Options), names)
                .UseEnrichmentSessions(new MartenEnrichmentSessionProvider(store));
    }

    extension(WallabyTestHarness harness)
    {
        /// <summary>Map a document to a sink/destination via a full transform (with <see cref="IQuerySession"/> access).</summary>
        public WallabyTestHarness Map<TEntity>(
            string sink,
            string? destination,
            Func<IQuerySession, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> transform,
            bool backfill = false,
            string? backfillVersion = null,
            Func<ChangeEvent, object?>? scopeKey = null,
            Func<object?, string?>? scopedDestination = null,
            Func<TEntity, object>? keyedBy = null)
            where TEntity : class
            => harness.AddMapping(new EntityMapping
            {
                EntityClrType = typeof(TEntity),
                SinkName = sink,
                Destination = destination,
                Transform = new MartenTransformInvoker<TEntity>(new DelegateTransform<TEntity>(transform)),
                Sessions = null!, // late-bound by the harness at StartAsync (UseTenantSessions may still override)
                ScopeKeySelector = scopeKey,
                DestinationSelector = scopedDestination,
                DocumentIdSelector = keyedBy is null ? null : KeyedBySelector(keyedBy),
            }, backfill, backfillVersion);

        /// <summary>Map a document to a sink/destination via a simple per-document projection.</summary>
        public WallabyTestHarness Project<TEntity>(
            string sink, string? destination, Func<TEntity, WallabyDocument?> document, bool backfill = false,
            string? backfillVersion = null, Func<ChangeEvent, object?>? scopeKey = null,
            Func<object?, string?>? scopedDestination = null, Func<TEntity, object>? keyedBy = null)
            where TEntity : class
            => harness.Map<TEntity>(sink, destination, (_, changes, _) =>
            {
                var documents = new Dictionary<DocumentKey, WallabyDocument?>();
                foreach (var change in changes)
                {
                    documents[change.Key] = document(change.Entity!);
                }
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
            }, backfill, backfillVersion, scopeKey, scopedDestination, keyedBy);

        /// <summary>Lease tenant-scoped query sessions from <paramref name="store"/> (for conjoined-tenancy tests).</summary>
        public WallabyTestHarness UseTenantSessions(IDocumentStore store)
            => harness.UseEnrichmentSessions(new MartenTenantSessionProvider(store));
    }

    /// <summary>The scope-key selector matching <c>ScopedByTenant()</c>: the captured <c>tenant_id</c>.</summary>
    public static Func<ChangeEvent, object?> TenantScopeKey { get; }
        = change => change.Record.GetValueOrDefault("TenantId");

    // Builds the selector through the real EntityMapBuilder so tests exercise production KeyedBy semantics.
    private static Func<ChangeEvent, string> KeyedBySelector<TEntity>(Func<TEntity, object> keyedBy)
        where TEntity : class
    {
        var registration = new DependencyInjection.MappingRegistration { EntityClrType = typeof(TEntity) };
        new DependencyInjection.EntityMapBuilder<TEntity>(registration).KeyedBy(keyedBy);
        return registration.DocumentIdSelector!;
    }
}
