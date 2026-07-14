using Npgsql;
using Wallaby.Internal;

namespace Wallaby.TestInfrastructure;

/// <summary>
/// Drops a test's replication slot and publication. Slots and publications survive on the shared
/// session container, and the fixture caps <c>max_replication_slots</c> — a test that creates them
/// must drop them or a later test's slot creation fails. Tests should not call this directly:
/// <see cref="ReplicationScope"/> runs it automatically on dispose.
/// </summary>
public static class PostgresReplicationCleanup
{
    /// <summary>
    /// Drop the slot and publication in <paramref name="names"/> if they exist. A just-stopped node's
    /// walsender can linger briefly after shutdown; retries until the server releases the slot.
    /// </summary>
    public static async Task DropAsync(string connectionString, WallabyNames names, CancellationToken ct = default)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await PgExec.ExecuteAsync(
                    connection,
                    "SELECT pg_drop_replication_slot(@s) WHERE EXISTS " +
                    "(SELECT 1 FROM pg_replication_slots WHERE slot_name = @s)",
                    ct,
                    ("s", names.Slot));
                break;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ObjectInUse && attempt < 50)
            {
                await Task.Delay(100, ct);
            }
        }

        await PgExec.ExecuteAsync(
            connection, $"DROP PUBLICATION IF EXISTS {PgExec.QuoteIdentifier(names.Publication)}", ct);
    }
}
