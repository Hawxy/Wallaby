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
    private readonly PostgreSqlContainer _container;

    private NpgsqlDataSource? _dataSource;

    public PostgresFixture() : this([])
    {
    }

    /// <summary>Start the container with extra server settings (each a <c>name=value</c> pair).</summary>
    protected PostgresFixture(string[] extraServerSettings)
    {
        var command = new List<string>
        {
            "-c", "wal_level=logical", "-c", "max_replication_slots=20", "-c", "max_wal_senders=20",
        };
        foreach (var setting in extraServerSettings)
        {
            command.Add("-c");
            command.Add(setting);
        }
        _container = new PostgreSqlBuilder("postgres:17").WithCommand([.. command]).Build();
    }

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
            await ReportLeakedSlotsAsync(_dataSource);
            await _dataSource.DisposeAsync();
        }
        await _container.DisposeAsync();
    }

    // The container caps max_replication_slots; a test that leaks its slot starves a later test's slot
    // creation with a timeout far from the cause. Name the leakers at session end instead.
    private static async Task ReportLeakedSlotsAsync(NpgsqlDataSource dataSource)
    {
        try
        {
            await using var command = dataSource.CreateCommand(
                "SELECT string_agg(slot_name, ', ') FROM pg_replication_slots");
            if (await command.ExecuteScalarAsync() is string slots)
            {
                Console.WriteLine($"[PostgresFixture] Replication slots leaked by this session's tests: {slots}");
            }
        }
        catch
        {
            // Diagnostics only — never fail teardown.
        }
    }
}
