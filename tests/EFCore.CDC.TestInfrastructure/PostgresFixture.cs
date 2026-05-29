using EFCore.CDC.TestModel;
using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;
using TUnit.Core.Interfaces;

namespace EFCore.CDC.Testing;

/// <summary>
/// A shared Postgres container started with <c>wal_level=logical</c> so the library can create a
/// pgoutput replication slot. The application schema (the <see cref="AppDbContext"/> tables) is created
/// once on startup so tests can capture/replicate them. Shared across the test session to keep runs fast.
/// </summary>
public sealed class PostgresFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17")
        .WithCommand("-c", "wal_level=logical", "-c", "max_replication_slots=20", "-c", "max_wal_senders=20")
        .Build();

    /// <summary>A normal connection string to the container (also usable for the replication connection).</summary>
    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        await using var ctx = new AppDbContext(TestModelFactory.CreateOptions(ConnectionString));
        await ctx.Database.EnsureCreatedAsync();
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
