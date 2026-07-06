using Marten;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Testing;
using Wallaby.TestInfrastructure.Marten;
using Weasel.Core;

namespace Wallaby.Marten.IntegrationTests;

/// <summary>
/// <c>ManageWallabyReplicaIdentity()</c> end to end: Marten's own schema apply sets
/// <c>REPLICA IDENTITY FULL</c> on the captured tables that need it, leaves the rest alone, and a
/// second migration pass detects nothing to do.
/// </summary>
[NotInParallel]
[ClassDataSource<MartenStoreFixture>(Shared = SharedType.PerTestSession)]
public class ReplicaIdentityFeatureTests(MartenStoreFixture pg)
{
    // A schema of this test's own, so the fixture's manual ALTER on the shared store can't mask the feature.
    private const string Schema = "ri_docs";

    [Test]
    public async Task Marten_schema_apply_sets_full_replica_identity_on_flagged_tables_only()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMarten(options =>
        {
            options.Connection(pg.ConnectionString);
            options.DatabaseSchemaName = Schema;
            options.RegisterDocumentType<Widget>();
            options.Schema.For<SoftWidget>().SoftDeleted();
        }).ManageWallabyReplicaIdentity();
        services.AddWallaby(cdc => cdc
            .UseMarten()
            .UseConnectionString(pg.ConnectionString)
            .AddSink(new CaptureSink())
            .WithMappings(sink =>
            {
                sink.Map<Widget>().ToDestination("widgets").UsingTransform((_, _, _) => Empty);
                sink.Map<SoftWidget>().ToDestination("softwidgets").UsingTransform((_, _, _) => Empty);
            }));

        await using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<IDocumentStore>();
        await store.Storage.ApplyAllConfiguredChangesToDatabaseAsync();

        // Soft-deleted documents are flagged (undelete TOAST fallback); plain documents stay on default.
        (await ReplicaIdentityAsync("mt_doc_softwidget")).ShouldBe('f');
        (await ReplicaIdentityAsync("mt_doc_widget")).ShouldBe('d');

        // The delta check sees the identity already applied, so a second pass is a no-op.
        var migration = await store.Storage.Database.CreateMigrationAsync();
        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    private static Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> Empty
        => Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(
            new Dictionary<DocumentKey, WallabyDocument?>());

    private async Task<char> ReplicaIdentityAsync(string table)
    {
        await using var connection = await pg.DataSource.OpenConnectionAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "select c.relreplident from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
            "where n.nspname = $1 and c.relname = $2";
        cmd.Parameters.AddWithValue(Schema);
        cmd.Parameters.AddWithValue(table);
        return (char)(await cmd.ExecuteScalarAsync())!;
    }
}
