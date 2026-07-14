using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Internal.Pipeline;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.TestInfrastructure.EntityFrameworkCore;

/// <summary>
/// EF Core test-model surface for <see cref="WallabyTestHarness"/>: the <c>ForTestModel</c> factory plus
/// <c>Map</c>/<c>Project</c>/<c>UseScopedContext</c> configuration and the <see cref="TestDatabase"/>
/// seed helpers, all against the shared <see cref="AppDbContext"/>.
/// </summary>
public static class WallabyTestHarnessEfCoreExtensions
{
    extension(WallabyTestHarness)
    {
        /// <summary>Create a harness wired to the shared <see cref="AppDbContext"/> test model.</summary>
        public static WallabyTestHarness ForTestModel(
            string connectionString, WallabyNames? names = null, Action<EfCoreProviderOptions>? configure = null)
        {
            var providerOptions = new EfCoreProviderOptions();
            configure?.Invoke(providerOptions);
            var contextFactory = () => new AppDbContext(TestModelFactory.CreateOptions(connectionString));
            using var context = contextFactory();
            return new WallabyTestHarness(
                    connectionString, new EfCoreModelProvider(context.Model, providerOptions.Exclusions), names)
                .UseEnrichmentSessions(new DbContextEnrichmentSessionProvider(contextFactory));
        }
    }

    extension(WallabyTestHarness harness)
    {
        /// <summary>Seed/mutation helpers for the <see cref="AppDbContext"/> test model on this harness's database.</summary>
        public TestDatabase Db => new(harness.ConnectionString);

        /// <summary>Map an entity to a sink/destination via a full transform (with <see cref="DbContext"/> access).</summary>
        public WallabyTestHarness Map<TEntity>(
            string sink,
            string? destination,
            Func<DbContext, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> transform,
            bool backfill = false,
            string? backfillVersion = null,
            Func<TEntity, object?>? scopeKey = null,
            Func<object?, string?>? scopedDestination = null,
            Func<TEntity, object>? keyedBy = null)
            where TEntity : class
            => harness.AddMapping(new EntityMapping
            {
                EntityClrType = typeof(TEntity),
                SinkName = sink,
                Destination = destination,
                Transform = new EfCoreTransformInvoker<TEntity>(new DelegateTransform<TEntity>(transform)),
                Sessions = null!, // late-bound by the harness at StartAsync (UseScopedContext may still override)
                ScopeKeySelector = scopeKey is null ? null : change => change.Entity is TEntity e ? scopeKey(e) : null,
                DestinationSelector = scopedDestination,
                DocumentIdSelector = keyedBy is null ? null : KeyedBySelector(keyedBy),
            }, backfill, backfillVersion);

        /// <summary>Map an entity to a sink/destination via a simple per-row projection.</summary>
        public WallabyTestHarness Project<TEntity>(
            string sink, string? destination, Func<TEntity, WallabyDocument?> document, bool backfill = false, string? backfillVersion = null,
            Func<TEntity, object?>? scopeKey = null, Func<object?, string?>? scopedDestination = null)
            where TEntity : class
            => harness.Map<TEntity>(sink, destination, (_, changes, _) =>
            {
                var documents = new Dictionary<DocumentKey, WallabyDocument?>();
                foreach (var change in changes)
                {
                    documents[change.Key] = document(change.Entity!);
                }
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
            }, backfill, backfillVersion, scopeKey, scopedDestination);

        /// <summary>Build the enrichment <see cref="DbContext"/> from a row's scope key (for tenant tests).</summary>
        public WallabyTestHarness UseScopedContext(Func<object?, DbContext> factory)
            => harness.UseEnrichmentSessions(
                new ScopedDbContextEnrichmentSessionProvider((key, _) => factory(key), NullServiceProvider.Instance));
    }

    /// <summary>The production <c>KeyedBy</c> document-id rule (a null selector result throws with DDL guidance).</summary>
    private static Func<ChangeEvent, string> KeyedBySelector<TEntity>(Func<TEntity, object> keyedBy) where TEntity : class
    {
        var registration = new MappingRegistration { EntityClrType = typeof(TEntity) };
        new EntityMapBuilder<TEntity>(registration).KeyedBy(keyedBy);
        return registration.DocumentIdSelector!;
    }

    private sealed class NullServiceProvider : IServiceProvider, IServiceScopeFactory, IServiceScope
    {
        public static readonly NullServiceProvider Instance = new();
        public object? GetService(Type serviceType) => serviceType == typeof(IServiceScopeFactory) ? this : null;
        public IServiceScope CreateScope() => this;
        public IServiceProvider ServiceProvider => this;
        public void Dispose() { }
    }
}
