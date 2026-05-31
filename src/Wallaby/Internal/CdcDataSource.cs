using Npgsql;

namespace Wallaby.Internal;

/// <summary>
/// Owns the single <see cref="NpgsqlDataSource"/> CDC uses for all pooled work (checkpoints, advisory
/// locks, backfill reads, dependent-key lookups). Built from the connection string supplied via
/// <see cref="DependencyInjection.CdcBuilder.UseConnectionString"/>; lifetime is tied to the DI container.
/// </summary>
internal sealed class CdcDataSource : IAsyncDisposable
{
    public CdcDataSource(string connectionString)
    {
        ConnectionString = connectionString;
        Source = NpgsqlDataSource.Create(connectionString);
    }

    /// <summary>The pooled data source CDC opens normal connections from.</summary>
    public NpgsqlDataSource Source { get; }

    /// <summary>
    /// The original connection string. <see cref="Npgsql.Replication.LogicalReplicationConnection"/> uses
    /// a separate Postgres protocol mode and cannot be obtained from <see cref="NpgsqlDataSource"/>'s pool,
    /// so it's built directly from this string.
    /// </summary>
    public string ConnectionString { get; }

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}
