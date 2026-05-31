using Microsoft.Extensions.Logging;
using Npgsql;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Validates that the Postgres server is configured for logical replication. Throws
/// <see cref="CdcConfigurationException"/> with actionable guidance when it is not. Never edits server
/// configuration.
/// </summary>
internal sealed class ServerValidator(ILogger logger)
{
    public async Task ValidateAsync(NpgsqlConnection connection, string slotName, CancellationToken ct)
    {
        var walLevel = await PgExec.ScalarStringAsync(connection, "SHOW wal_level", ct);
        if (!string.Equals(walLevel, "logical", StringComparison.OrdinalIgnoreCase))
        {
            throw new CdcConfigurationException(
                $"Postgres 'wal_level' is '{walLevel}', but logical replication requires 'logical'. " +
                "Set wal_level=logical (in postgresql.conf or your managed-instance parameter group) and restart the server.");
        }

        var hasReplication = await PgExec.ScalarBoolAsync(
            connection,
            "SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user",
            ct);
        if (!hasReplication)
        {
            throw new CdcConfigurationException(
                "The current Postgres role lacks the REPLICATION attribute and is not a superuser. " +
                "Grant it with: ALTER ROLE <role> WITH REPLICATION;");
        }

        var maxSlots = await PgExec.ScalarLongAsync(connection, "SELECT setting::int FROM pg_settings WHERE name = 'max_replication_slots'", ct);
        var usedSlots = await PgExec.ScalarLongAsync(connection, "SELECT count(*) FROM pg_replication_slots", ct);
        var slotExists = await PgExec.ScalarLongAsync(
            connection, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = @s", ct, ("s", slotName)) > 0;

        if (!slotExists && usedSlots >= maxSlots)
        {
            throw new CdcConfigurationException(
                $"No logical replication slot headroom: max_replication_slots={maxSlots}, in use={usedSlots}. " +
                "Increase max_replication_slots or drop unused slots.");
        }

        var maxWalSenders = await PgExec.ScalarLongAsync(connection, "SELECT setting::int FROM pg_settings WHERE name = 'max_wal_senders'", ct);
        if (maxWalSenders <= 0)
        {
            throw new CdcConfigurationException(
                "max_wal_senders is 0; logical replication needs at least one WAL sender. Increase max_wal_senders and restart.");
        }

        logger.LogInformation(
            "CDC server validation passed (wal_level=logical, max_replication_slots={MaxSlots}, in use={UsedSlots}).",
            maxSlots, usedSlots);
    }
}
