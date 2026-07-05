using Npgsql;
using Testcontainers.PostgreSql;
using TUnit.Core.Interfaces;

namespace Wallaby.TestInfrastructure;

/// <summary>
/// A shared Postgres container started with <c>wal_level=logical</c> so the library can create a
/// pgoutput replication slot. Shared across the test session to keep runs fast. Derive from it to
/// bootstrap an application schema (see the EF Core test model's <c>TestModelPostgresFixture</c>).
/// </summary>
public class PostgresFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17")
        .WithCommand("-c", "wal_level=logical", "-c", "max_replication_slots=20", "-c", "max_wal_senders=20")
        .Build();

    private NpgsqlDataSource? _dataSource;

    /// <summary>A normal connection string to the container (also usable for the replication connection).</summary>
    public string ConnectionString => _container.GetConnectionString();

    /// <summary>A shared <see cref="NpgsqlDataSource"/> pointing at the container.</summary>
    public NpgsqlDataSource DataSource => _dataSource
        ?? throw new InvalidOperationException("PostgresFixture has not been initialized.");

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _dataSource = NpgsqlDataSource.Create(ConnectionString);
        await BootstrapAsync(ConnectionString);
    }

    /// <summary>Create the application schema the tests capture; the base fixture creates none.</summary>
    protected virtual Task BootstrapAsync(string connectionString) => Task.CompletedTask;

    public virtual async ValueTask DisposeAsync()
    {
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
        }
        await _container.DisposeAsync();
    }
}
