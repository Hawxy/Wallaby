using Npgsql;
using Testcontainers.PostgreSql;
using TUnit.Core.Interfaces;

namespace Wallaby.Sinks.Pgvector.Tests.Integration.Infrastructure;

/// <summary>A shared pgvector-enabled Postgres container acting as the sink destination.</summary>
public sealed class PgvectorFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("pgvector/pgvector:pg17").Build();
    private NpgsqlDataSource? _dataSource;

    public string ConnectionString => _container.GetConnectionString();

    public NpgsqlDataSource DataSource => _dataSource
        ?? throw new InvalidOperationException("PgvectorFixture has not been initialized.");

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _dataSource = NpgsqlDataSource.Create(ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
        }
        await _container.DisposeAsync();
    }
}
