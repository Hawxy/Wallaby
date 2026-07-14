using Microsoft.Extensions.Logging;
using Npgsql;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Creates or adopts a pgoutput logical replication slot and records it in <c>wallaby.slot_registry</c>.
/// A pre-existing slot of the wrong type fails fast; a server-invalidated slot (wal_status=lost) is
/// dropped and recreated.
/// </summary>
internal sealed class SlotProvisioner(ILogger logger)
{
    public async Task<(bool Created, string? ConsistentPoint)> EnsureAsync(
        NpgsqlConnection connection, string slot, string publication, string kind, CancellationToken ct)
    {
        var existing = await GetSlotAsync(connection, slot, ct);
        if (existing is not null)
        {
            var (slotType, plugin, walStatus) = existing.Value;

            // Adopt a slot we didn't create this run. It must be a pgoutput logical slot — anything else
            // (a physical slot, or a logical slot on a different output plugin) can't serve this slot's
            // purpose, so fail fast rather than silently assuming it matches the declaration.
            if (!string.Equals(slotType, "logical", StringComparison.Ordinal) ||
                !string.Equals(plugin, "pgoutput", StringComparison.Ordinal))
            {
                throw new WallabyConfigurationException(
                    $"Replication slot '{slot}' already exists but is not a pgoutput logical slot " +
                    $"(slot_type='{slotType}', plugin='{plugin ?? "<none>"}'). Wallaby requires a logical/pgoutput " +
                    $"slot. Drop it with SELECT pg_drop_replication_slot('{slot}'); or use a different slot name.");
            }

            if (!string.Equals(walStatus, "lost", StringComparison.Ordinal))
            {
                // Record the adopted slot so wallaby.slot_registry reflects reality (we don't know its original
                // consistent point, so keep any value already recorded).
                await UpsertSlotRegistryAsync(connection, slot, publication, consistentPoint: null, kind, ct);
                return (false, null);
            }

            // The server invalidated the slot (e.g. max_slot_wal_keep_size exceeded); its WAL is gone and
            // streaming from it can never resume. Recreate it — the caller repairs the missed window via
            // checkpoint gap detection and re-backfill.
            logger.SlotInvalidated(slot);
            await PgExec.ExecuteAsync(connection, "SELECT pg_drop_replication_slot(@s)", ct, ("s", slot));
        }

        var consistentPoint = await PgExec.ScalarStringAsync(
            connection, "SELECT lsn::text FROM pg_create_logical_replication_slot(@s, 'pgoutput')", ct, ("s", slot));

        await UpsertSlotRegistryAsync(connection, slot, publication, consistentPoint, kind, ct);

        logger.SlotCreated(slot, consistentPoint);
        return (true, consistentPoint);
    }

    private static async Task<(string SlotType, string? Plugin, string? WalStatus)?> GetSlotAsync(
        NpgsqlConnection connection, string slot, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT slot_type, plugin, wal_status::text FROM pg_replication_slots WHERE slot_name = @s", connection);
        cmd.Parameters.AddWithValue("s", slot);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        var slotType = reader.GetString(0);
        var plugin = reader.IsDBNull(1) ? null : reader.GetString(1);
        var walStatus = reader.IsDBNull(2) ? null : reader.GetString(2);
        return (slotType, plugin, walStatus);
    }

    private static Task UpsertSlotRegistryAsync(
        NpgsqlConnection connection, string slot, string publication, string? consistentPoint, string kind, CancellationToken ct)
        => PgExec.ExecuteAsync(
            connection,
            """
            INSERT INTO wallaby.slot_registry (slot_name, publication, consistent_point, kind)
            VALUES (@s, @p, @cp::pg_lsn, @k)
            ON CONFLICT (slot_name) DO UPDATE
                SET publication = EXCLUDED.publication,
                    consistent_point = COALESCE(EXCLUDED.consistent_point, slot_registry.consistent_point),
                    kind = EXCLUDED.kind
            """,
            ct,
            ("s", slot), ("p", publication), ("cp", consistentPoint), ("k", kind));
}

/// <summary>Source-generated log messages for <see cref="SlotProvisioner"/>.</summary>
internal static partial class SlotProvisionerLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Created pgoutput replication slot '{Slot}' at {ConsistentPoint}.")]
    internal static partial void SlotCreated(this ILogger logger, string slot, string? consistentPoint);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Replication slot '{Slot}' was invalidated by the server (wal_status=lost); dropping and recreating it.")]
    internal static partial void SlotInvalidated(this ILogger logger, string slot);
}
