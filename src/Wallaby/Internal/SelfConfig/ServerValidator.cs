using Microsoft.Extensions.Logging;
using Npgsql;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Validates that the Postgres server is configured for logical replication. Throws
/// <see cref="WallabyConfigurationException"/> with actionable guidance when it is not. Never edits server
/// configuration.
/// </summary>
internal sealed class ServerValidator(ILogger logger)
{
    public async Task ValidateAsync(NpgsqlConnection connection, IReadOnlyCollection<string> slotNames, CancellationToken ct)
    {
        var versionNum = await PgExec.ScalarLongAsync(
            connection, "SELECT current_setting('server_version_num')::int", ct);
        if (versionNum < 150000)
        {
            var display = await PgExec.ScalarStringAsync(connection, "SHOW server_version", ct);
            throw new WallabyConfigurationException(
                $"Postgres server version {display} is not supported. Wallaby requires PostgreSQL 15 or later " +
                "(publication column lists). Upgrade the server, or pin an older Wallaby release for PostgreSQL 14.");
        }

        var walLevel = await PgExec.ScalarStringAsync(connection, "SHOW wal_level", ct);
        if (!string.Equals(walLevel, "logical", StringComparison.OrdinalIgnoreCase))
        {
            throw new WallabyConfigurationException(
                $"Postgres 'wal_level' is '{walLevel}', but logical replication requires 'logical'. " +
                "Set wal_level=logical (in postgresql.conf or your managed-instance parameter group) and restart the server.");
        }
        var hasReplication = await PgExec.ScalarBoolAsync(
            connection,
            """
            SELECT bool_or(ok) FROM (
                SELECT rolreplication OR rolsuper AS ok
                FROM pg_roles WHERE rolname = current_user
                UNION ALL
                SELECT pg_has_role(current_user, oid, 'MEMBER')
                FROM pg_roles
                WHERE rolname IN ('rds_replication', 'rds_superuser', 'azure_pg_admin', 'cloudsqlsuperuser', 'cloudsqlreplica')
            ) s
            """,
            ct);
        if (!hasReplication)
        {
            var role = await PgExec.ScalarStringAsync(connection, "SELECT current_user", ct);
            logger.ReplicationPrivilegeUnverified(role);
        }

        var maxSlots = await PgExec.ScalarLongAsync(connection, "SELECT setting::int FROM pg_settings WHERE name = 'max_replication_slots'", ct);
        var usedSlots = await PgExec.ScalarLongAsync(connection, "SELECT count(*) FROM pg_replication_slots", ct);

        // Only slots that don't already exist will consume headroom
        var alreadyExisting = await PgExec.ScalarLongAsync(
            connection, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = ANY(@names)", ct,
            ("names", slotNames.ToArray()));
        var toCreate = slotNames.Count - alreadyExisting;

        if (usedSlots + toCreate > maxSlots)
        {
            throw new WallabyConfigurationException(
                $"No logical replication slot headroom: max_replication_slots={maxSlots}, in use={usedSlots}, " +
                $"need to create {toCreate} more. Increase max_replication_slots or drop unused slots.");
        }

        var maxWalSenders = await PgExec.ScalarLongAsync(connection, "SELECT setting::int FROM pg_settings WHERE name = 'max_wal_senders'", ct);
        if (maxWalSenders <= 0)
        {
            throw new WallabyConfigurationException(
                "max_wal_senders is 0; logical replication needs at least one WAL sender. Increase max_wal_senders and restart.");
        }

        logger.ServerValidationPassed(maxSlots, usedSlots);
    }
}

/// <summary>Source-generated log messages for <see cref="ServerValidator"/>.</summary>
internal static partial class ServerValidatorLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby server validation passed (wal_level=logical, max_replication_slots={MaxSlots}, in use={UsedSlots}).")]
    internal static partial void ServerValidationPassed(this ILogger logger, long maxSlots, long usedSlots);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not verify the REPLICATION privilege for role {Role}; logical replication will fail to start without it. Grant it with 'ALTER ROLE ... WITH REPLICATION' or 'GRANT rds_replication TO ...' ")]
    internal static partial void ReplicationPrivilegeUnverified(this ILogger logger, string? role);
}
