using Npgsql;
using Wallaby.DependencyInjection;

namespace Wallaby.Internal;

/// <summary>
/// Owns the single <see cref="NpgsqlDataSource"/> Wallaby uses for all pooled work (checkpoints, advisory
/// locks, backfill reads, dependent-key lookups). Built from the connection string supplied via
/// <see cref="WallabyBuilder.UseConnectionString(string)"/>; lifetime is tied to the DI container.
/// </summary>
internal sealed class WallabyDataSource : IAsyncDisposable
{
    public WallabyDataSource(string connectionString)
    {
        ConnectionString = connectionString;
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        // Auto-prepare the hot bookkeeping statements (checkpoint upsert, fanout queue, control
        // reads) unless the consumer configured auto-prepare explicitly.
        if (builder.MaxAutoPrepare == 0 && !builder.ShouldSerialize("Max Auto Prepare"))
        {
            builder.MaxAutoPrepare = 64;
        }
        // NULL array elements throw at read time under Npgsql's default ArrayNullabilityMode.Never;
        // PerInstance reads them as Nullable<T>[] instead (backfill reads share the replication
        // stream's decoding behavior). Applied unless the consumer configured the mode explicitly.
        if (!builder.ShouldSerialize("Array Nullability Mode"))
        {
            builder.ArrayNullabilityMode = ArrayNullabilityMode.PerInstance;
        }
        var source = NpgsqlDataSource.Create(builder);

        if(source is NpgsqlMultiHostDataSource multiHostDataSource)
            Source = multiHostDataSource.WithTargetSession(TargetSessionAttributes.Primary);
        else
            Source = source;
    }

    /// <summary>The pooled data source Wallaby opens normal connections from.</summary>
    public NpgsqlDataSource Source { get; }

    /// <summary>
    /// The original connection string. <see cref="Npgsql.Replication.LogicalReplicationConnection"/> uses
    /// a separate Postgres protocol mode and cannot be obtained from <see cref="NpgsqlDataSource"/>'s pool,
    /// so it's built directly from this string.
    /// </summary>
    public string ConnectionString { get; }

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}
