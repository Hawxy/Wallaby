using Marten;
using Wallaby.TestInfrastructure;

namespace Wallaby.TestInfrastructure.Marten;

/// <summary>
/// A <see cref="PostgresFixture"/> that also builds the Marten test store and applies its schema
/// (document tables and upsert functions), for suites that capture the Marten test documents.
/// The store is shared by the harness (model + sessions) and the tests (seeding documents).
/// </summary>
public sealed class MartenStoreFixture : PostgresFixture
{
    private DocumentStore? _store;

    public DocumentStore Store => _store
        ?? throw new InvalidOperationException("MartenStoreFixture has not been initialized.");

    protected override async Task BootstrapAsync(string connectionString)
    {
        _store = MartenTestStore.Create(connectionString);
        await _store.Storage.ApplyAllConfiguredChangesToDatabaseAsync();

        // Wallaby never alters replica identity itself (self-config only warns); apply the DDL the way a
        // consumer would for the soft-deleted document, whose undeletes need the old tuple on the wire.
        await using var connection = new Npgsql.NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new Npgsql.NpgsqlCommand(
            $"ALTER TABLE {MartenTestStore.Schema}.mt_doc_softwidget REPLICA IDENTITY FULL", connection);
        await cmd.ExecuteNonQueryAsync();
    }

    public override async ValueTask DisposeAsync()
    {
        _store?.Dispose();
        await base.DisposeAsync();
    }
}
