namespace Wallaby.TestInfrastructure;

/// <summary>
/// Unique slot/publication names whose disposal drops both via <see cref="PostgresReplicationCleanup"/>.
/// Non-harness (full-DI) tests must use this instead of a bare <see cref="WallabyNames.Unique"/>:
/// declare it with <c>await using</c> <b>before</b> starting any <see cref="WallabyTestNode"/> so the
/// drop runs after every node has stopped — a leaked slot starves <c>max_replication_slots</c> and a
/// leaked publication's column lists break other tests' DML on the shared database.
/// </summary>
public sealed class ReplicationScope : IAsyncDisposable
{
    private readonly string _connectionString;

    private ReplicationScope(string connectionString, WallabyNames names)
    {
        _connectionString = connectionString;
        Names = names;
    }

    /// <summary>Unique names on the given database, dropped when the scope disposes.</summary>
    public static ReplicationScope Unique(string connectionString) => new(connectionString, WallabyNames.Unique());

    public WallabyNames Names { get; }

    public string Suffix => Names.Suffix;

    public string Slot => Names.Slot;

    public string Publication => Names.Publication;

    /// <inheritdoc cref="WallabyNames.Named"/>
    public string Named(string prefix) => Names.Named(prefix);

    /// <summary>The scope passes as its names wherever a <see cref="WallabyNames"/> is expected.</summary>
    public static implicit operator WallabyNames(ReplicationScope scope) => scope.Names;

    private readonly List<WallabyNames> _extra = [];

    /// <summary>Register an additional slot/publication pair (e.g. an external slot) to drop with the scope.</summary>
    public void TrackExternal(string slot, string publication)
        => _extra.Add(new WallabyNames(Suffix, slot, publication));

    public async ValueTask DisposeAsync()
    {
        await PostgresReplicationCleanup.DropAsync(_connectionString, Names);
        foreach (var extra in _extra)
        {
            await PostgresReplicationCleanup.DropAsync(_connectionString, extra);
        }
    }
}
