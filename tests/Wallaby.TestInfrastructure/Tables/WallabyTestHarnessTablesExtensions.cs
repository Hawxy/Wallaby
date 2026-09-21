using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Providers.Tables;
using Wallaby.Providers.Tables.Internal;

namespace Wallaby.TestInfrastructure.Tables;

/// <summary>
/// Plain-table surface for <see cref="WallabyTestHarness"/>: the <c>ForTables</c> factories plus
/// <c>Map</c>/<c>Project</c> against the shared test rows.
/// </summary>
public static class WallabyTestHarnessTablesExtensions
{
    extension(WallabyTestHarness)
    {
        /// <summary>Create a harness over the registrations in <paramref name="configure"/>, handing transforms <paramref name="dataSource"/>.</summary>
        public static WallabyTestHarness ForTables(
            string connectionString, NpgsqlDataSource dataSource, Action<TablesModelBuilder> configure, WallabyNames? names = null)
        {
            var model = new TablesModelBuilder();
            configure(model);
            return new WallabyTestHarness(connectionString, new TablesModelProvider(model.Build()), names)
                .UseEnrichmentSessions(new TablesEnrichmentSessionProvider(dataSource));
        }

        /// <summary>Create a harness over the fixture's shared registrations.</summary>
        public static WallabyTestHarness ForTestTables(TablesFixture pg, WallabyNames? names = null)
            => WallabyTestHarness.ForTables(pg.ConnectionString, pg.DataSource, TablesFixture.Configure, names);
    }

    extension(WallabyTestHarness harness)
    {
        /// <summary>Map a row type to a sink/destination via a full transform (with <see cref="NpgsqlDataSource"/> access).</summary>
        public WallabyTestHarness Map<TEntity>(
            string sink,
            string? destination,
            Func<NpgsqlDataSource, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> transform,
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
                Transform = new TablesTransformInvoker<TEntity>(new DelegateTransform<TEntity>(transform)),
                Sessions = null!, // late-bound by the harness at StartAsync
                ScopeKeySelector = scopeKey,
                DestinationSelector = scopedDestination,
                DocumentIdSelector = keyedBy is null ? null : KeyedBySelector(keyedBy),
            }, backfill, backfillVersion);

        /// <summary>Map a row type to a sink/destination via a simple per-row projection.</summary>
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
    }

    // Builds the selector through the real EntityMapBuilder so tests exercise production KeyedBy semantics.
    private static Func<ChangeEvent, string> KeyedBySelector<TEntity>(Func<TEntity, object> keyedBy)
        where TEntity : class
    {
        var registration = new DependencyInjection.MappingRegistration { EntityClrType = typeof(TEntity) };
        new DependencyInjection.EntityMapBuilder<TEntity>(registration).KeyedBy(keyedBy);
        return registration.DocumentIdSelector!;
    }
}
