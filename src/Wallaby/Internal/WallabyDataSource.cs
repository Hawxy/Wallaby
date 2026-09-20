using Npgsql;
using Wallaby.DependencyInjection;

namespace Wallaby.Internal;

/// <summary>
/// Owns the single <see cref="NpgsqlDataSource"/> Wallaby uses for all pooled work (checkpoints, advisory
/// locks, backfill reads, dependent-key lookups) and supplies the credentials for the replication
/// connection, which cannot come from the pool. Built from the connection string supplied via
/// <see cref="WallabyBuilder.UseConnectionString(string)"/>; lifetime is tied to the DI container.
/// </summary>
internal sealed class WallabyDataSource : IAsyncDisposable
{
    /// <summary>Inside the shortest common token lifetime (15 minutes for RDS IAM), leaving room for clock skew.</summary>
    public static readonly TimeSpan DefaultPasswordRefreshInterval = TimeSpan.FromMinutes(5);

    private static readonly TimeSpan PasswordFailureRefreshInterval = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan DefaultPasswordFetchTimeout = TimeSpan.FromSeconds(30);

    private readonly Func<CancellationToken, ValueTask<string>>? _passwordProvider;
    private readonly TimeSpan _passwordFetchTimeout;

    public WallabyDataSource(
        string connectionString,
        Func<CancellationToken, ValueTask<string>>? passwordProvider = null,
        TimeSpan? passwordRefreshInterval = null,
        Action<NpgsqlDataSourceBuilder>? configureDataSource = null,
        TimeSpan? passwordFetchTimeout = null)
    {
        ConnectionString = connectionString;
        _passwordProvider = passwordProvider;
        _passwordFetchTimeout = passwordFetchTimeout ?? DefaultPasswordFetchTimeout;

        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (passwordProvider is not null && (builder.Password is not null || builder.Passfile is not null))
        {
            throw new WallabyConfigurationException(
                "UsePasswordProvider(...) supplies the password, so the connection string must not set Password or Passfile.");
        }
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

        var sourceBuilder = new NpgsqlDataSourceBuilder(builder.ConnectionString);
        if (passwordProvider is not null)
        {
            // Timer-driven: the token is refreshed off the open path, so opens never wait on a token fetch.
            sourceBuilder.UsePeriodicPasswordProvider(
                (_, ct) => passwordProvider(ct),
                passwordRefreshInterval ?? DefaultPasswordRefreshInterval,
                PasswordFailureRefreshInterval);
        }
        configureDataSource?.Invoke(sourceBuilder);
        var source = sourceBuilder.Build();

        if (source is NpgsqlMultiHostDataSource multiHostDataSource)
            Source = multiHostDataSource.WithTargetSession(TargetSessionAttributes.Primary);
        else
            Source = source;
    }

    public NpgsqlDataSource Source { get; }

    /// <summary>
    /// The original connection string. <see cref="Npgsql.Replication.LogicalReplicationConnection"/> uses
    /// a separate Postgres protocol mode and cannot be obtained from <see cref="NpgsqlDataSource"/>'s pool,
    /// so it's built directly from this string.
    /// </summary>
    public string ConnectionString { get; }

    /// <summary>
    /// <see cref="ConnectionString"/> with the password provider's current token embedded, or the original
    /// string when no provider is set. Consulted once per leader term for the replication connection and
    /// the primary probes. The fetch is bounded to 30 seconds so a hung token service fails the term
    /// (and retries with the leader backoff) instead of stalling it.
    /// </summary>
    public async ValueTask<string> ConnectionStringWithPasswordAsync(CancellationToken ct)
    {
        if (_passwordProvider is null)
        {
            return ConnectionString;
        }

        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(_passwordFetchTimeout);
        string password;
        try
        {
            password = await _passwordProvider(bounded.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException($"The password provider did not return a password within {_passwordFetchTimeout}.");
        }
        if (string.IsNullOrEmpty(password))
        {
            throw new InvalidOperationException("The password provider returned no password.");
        }
        return new NpgsqlConnectionStringBuilder(ConnectionString) { Password = password }.ConnectionString;
    }

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}
