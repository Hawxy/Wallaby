using Npgsql;
using NpgsqlTypes;
using Wallaby.Abstractions;

namespace Wallaby.Internal.State;

/// <summary>
/// Stores the replication checkpoint in <c>cdc.checkpoint</c> as a <c>pg_lsn</c>. This is supplementary
/// bookkeeping; the authoritative resume position is the slot's <c>confirmed_flush_lsn</c> on the server.
/// </summary>
internal sealed class PostgresCheckpointStore(NpgsqlDataSource dataSource) : ICheckpointStore
{
    public async Task<Checkpoint?> GetAsync(string slotName, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand(
            "SELECT confirmed_lsn, updated_at FROM cdc.checkpoint WHERE slot_name = @s", connection);
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
            INSERT INTO cdc.checkpoint (slot_name, confirmed_lsn, updated_at)
            VALUES (@s, @lsn::pg_lsn, now())
            ON CONFLICT (slot_name) DO UPDATE
                SET confirmed_lsn = EXCLUDED.confirmed_lsn,
                    updated_at = EXCLUDED.updated_at
            """,
            connection);
        cmd.Parameters.AddWithValue("s", slotName);
        cmd.Parameters.AddWithValue("lsn", new NpgsqlLogSequenceNumber(checkpoint.ConfirmedLsn).ToString());
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
