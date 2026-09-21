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
    /// <summary>
    /// Ensures the slot exists. <c>Recreated</c> is true when the slot was created this call but a
    /// <c>wallaby.slot_registry</c> row for it already existed: the installation had a slot before
    /// (suspension finalize, manual drop, server invalidation), so stream continuity cannot be assumed.
    /// </summary>
    public async Task<(bool Created, string? ConsistentPoint, bool Recreated)> EnsureAsync(
        NpgsqlConnection connection, string slot, string publication, string kind, bool publicationManaged,
        CancellationToken ct)
    {
        var existing = await GetSlotAsync(connection, slot, ct);
        if (existing is not null)
        {
            var (slotType, plugin, walStatus, invalidationReason) = existing.Value;

            // Adopting an existing slot requires a pgoutput logical slot; a physical slot or a different
            // output plugin can't serve this purpose, so fail fast.
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
                await UpsertSlotRegistryAsync(
                    connection, slot, publication, consistentPoint: null, kind, publicationManaged, ct);
                return (false, null, false);
            }

            // The server invalidated the slot (max_slot_wal_keep_size exceeded, idle_replication_slot_timeout
            // on PG18+, ...); streaming from it can never resume. Recreate it: the caller repairs the missed
            // window via checkpoint gap detection and re-backfill.
            logger.SlotInvalidated(slot, invalidationReason ?? "unknown");
            await PgExec.ExecuteAsync(connection, "SELECT pg_drop_replication_slot(@s)", ct, ("s", slot));
        }

        var alreadyRegistered = await PgExec.ScalarBoolAsync(
            connection, "SELECT EXISTS (SELECT 1 FROM wallaby.slot_registry WHERE slot_name = @s)", ct, ("s", slot));

        var consistentPoint = await PgExec.ScalarStringAsync(
            connection, "SELECT lsn::text FROM pg_create_logical_replication_slot(@s, 'pgoutput')", ct, ("s", slot));

        await UpsertSlotRegistryAsync(connection, slot, publication, consistentPoint, kind, publicationManaged, ct);

        logger.SlotCreated(slot, consistentPoint);
        return (true, consistentPoint, alreadyRegistered);
    }

    // invalidation_reason exists from PG17; the jsonb projection reads null on older servers.
    private static async Task<(string SlotType, string? Plugin, string? WalStatus, string? InvalidationReason)?> GetSlotAsync(
        NpgsqlConnection connection, string slot, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            """
            SELECT slot_type, plugin, wal_status::text, to_jsonb(s) ->> 'invalidation_reason'
            FROM pg_replication_slots s WHERE slot_name = @s
            """, connection);
        cmd.Parameters.AddWithValue("s", slot);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        var slotType = reader.GetString(0);
        var plugin = reader.IsDBNull(1) ? null : reader.GetString(1);
        var walStatus = reader.IsDBNull(2) ? null : reader.GetString(2);
        var invalidationReason = reader.IsDBNull(3) ? null : reader.GetString(3);
        return (slotType, plugin, walStatus, invalidationReason);
    }

    private static Task UpsertSlotRegistryAsync(
        NpgsqlConnection connection, string slot, string publication, string? consistentPoint, string kind,
        bool publicationManaged, CancellationToken ct)
        => PgExec.ExecuteAsync(
            connection,
            """
            INSERT INTO wallaby.slot_registry (slot_name, publication, consistent_point, kind, publication_managed)
            VALUES (@s, @p, @cp::pg_lsn, @k, @m)
            ON CONFLICT (slot_name) DO UPDATE
                SET publication = EXCLUDED.publication,
                    consistent_point = COALESCE(EXCLUDED.consistent_point, slot_registry.consistent_point),
                    kind = EXCLUDED.kind,
                    publication_managed = EXCLUDED.publication_managed
            """,
            ct,
            ("s", slot), ("p", publication), ("cp", consistentPoint), ("k", kind), ("m", publicationManaged));
}

internal static partial class SlotProvisionerLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Created pgoutput replication slot {Slot} at {ConsistentPoint}.")]
    internal static partial void SlotCreated(this ILogger logger, string slot, string? consistentPoint);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Replication slot {Slot} was invalidated by the server (wal_status=lost, invalidation_reason={Reason}); dropping and recreating it.")]
    internal static partial void SlotInvalidated(this ILogger logger, string slot, string reason);
}
