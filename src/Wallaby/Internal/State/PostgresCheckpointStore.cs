using Npgsql;
using NpgsqlTypes;
using Wallaby.Abstractions;

namespace Wallaby.Internal.State;

/// <summary>
/// Stores the replication checkpoint on the slot's <c>wallaby.slot_registry</c> row as a <c>pg_lsn</c>.
/// This is supplementary bookkeeping; the authoritative resume position is the slot's
/// <c>confirmed_flush_lsn</c> on the server. The provisioner registers every slot before its first
/// checkpoint write, so the save is a plain UPDATE.
/// </summary>
internal sealed class PostgresCheckpointStore(NpgsqlDataSource dataSource) : ICheckpointStore
{
    public async Task<Checkpoint?> GetAsync(string slotName, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand(
            """
            SELECT confirmed_lsn, checkpointed_at FROM wallaby.slot_registry
            WHERE slot_name = @s AND confirmed_lsn IS NOT NULL
            """, connection);
        cmd.Parameters.AddWithValue("s", slotName);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        var lsn = (ulong)reader.GetFieldValue<NpgsqlLogSequenceNumber>(0);
        var updatedAt = reader.GetFieldValue<DateTime>(1);
        return new Checkpoint(lsn, new DateTimeOffset(DateTime.SpecifyKind(updatedAt, DateTimeKind.Utc)));
    }

    public async Task SaveAsync(string slotName, Checkpoint checkpoint, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand(
            """
            UPDATE wallaby.slot_registry
            SET confirmed_lsn = @lsn::pg_lsn, checkpointed_at = now()
            WHERE slot_name = @s
            """,
            connection);
        cmd.Parameters.AddWithValue("s", slotName);
        cmd.Parameters.AddWithValue("lsn", new NpgsqlLogSequenceNumber(checkpoint.ConfirmedLsn).ToString());
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
