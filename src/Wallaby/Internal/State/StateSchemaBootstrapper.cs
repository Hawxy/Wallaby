using Microsoft.Extensions.Logging;
using Npgsql;
using Wallaby.Internal.Cluster;

namespace Wallaby.Internal.State;

/// <summary>
/// Creates and migrates the internal <c>wallaby</c> schema. State is co-located in the source database
/// so backfill watermarking and checkpointing observe a consistent view of the data. Applied
/// <see cref="StateSchemaMigrations"/> steps are recorded in <c>wallaby.schema_version</c>; a database
/// whose schema version is newer than this build fails fast (never run a downgraded binary against a
/// migrated schema). At the current version the bootstrap is a single read and runs no DDL.
/// Migrations are serialized across nodes by a dedicated advisory lock: the leader, the provision-only
/// service, and the configuration-suspend gate can all bootstrap concurrently.
/// </summary>
internal sealed class StateSchemaBootstrapper(ILogger? logger = null)
{
    // Distinct from both leadership lock keys; pinned the same way (see PostgresAdvisoryLock.StableKey).
    private static readonly long MigrationLockKey = PostgresAdvisoryLock.StableKey("wallaby_schema_migration");

    private static readonly string AppliedBy = WallabyVersion.Current;

    private const string VersionTableDdl = """
        CREATE SCHEMA IF NOT EXISTS wallaby;

        CREATE TABLE IF NOT EXISTS wallaby.schema_version (
            version    int         PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now(),
            applied_by text        NOT NULL
        );
        """;

    private const string ReadVersionSql = "SELECT coalesce(max(version), 0) FROM wallaby.schema_version";

    public Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct)
        => EnsureAsync(connection, StateSchemaMigrations.Steps, StateSchemaMigrations.CurrentVersion, ct);

    internal async Task EnsureAsync(
        NpgsqlConnection connection, IReadOnlyList<(int Version, string Ddl)> steps, int currentVersion,
        CancellationToken ct)
    {
        var dbVersion = await TryReadVersionAsync(connection, ct);
        GuardNewerSchema(dbVersion, currentVersion);
        if (dbVersion == currentVersion)
        {
            return;
        }

        // The ledger and all DDL are created under the migration lock, so concurrent bootstrappers
        // serialize here instead of racing CREATE TABLE. The lock is transaction-scoped and the whole
        // pending range applies atomically: an interrupted run leaves the previous version intact.
        await using var tx = await connection.BeginTransactionAsync(ct);
        await PgExec.ExecuteAsync(connection, "SELECT pg_advisory_xact_lock(@key)", ct, ("key", MigrationLockKey));
        await PgExec.ExecuteAsync(connection, VersionTableDdl, ct);

        // Another node may have migrated while this one waited on the lock.
        var lockedVersion = await PgExec.ScalarLongAsync(connection, ReadVersionSql, ct);
        GuardNewerSchema(lockedVersion, currentVersion);
        if (lockedVersion == currentVersion)
        {
            await tx.CommitAsync(ct);
            return;
        }

        foreach (var (version, ddl) in steps)
        {
            if (version <= lockedVersion)
            {
                continue;
            }

            await PgExec.ExecuteAsync(connection, ddl, ct);
            await PgExec.ExecuteAsync(
                connection,
                "INSERT INTO wallaby.schema_version (version, applied_by) VALUES (@v, @by)",
                ct, ("v", version), ("by", AppliedBy));
        }

        await tx.CommitAsync(ct);
        logger?.SchemaMigrated(lockedVersion, currentVersion);
    }

    // Missing ledger (or missing schema entirely) reads as version 0: a fresh database, or one
    // bootstrapped by a pre-versioning beta; both adopted by the baseline step.
    private static async Task<long> TryReadVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        try
        {
            return await PgExec.ScalarLongAsync(connection, ReadVersionSql, ct);
        }
        catch (PostgresException ex) when (
            ex.SqlState is PostgresErrorCodes.UndefinedTable or PostgresErrorCodes.InvalidSchemaName)
        {
            return 0;
        }
    }

    private static void GuardNewerSchema(long dbVersion, int currentVersion)
    {
        if (dbVersion > currentVersion)
        {
            throw new WallabyConfigurationException(
                $"The wallaby state schema is at version {dbVersion}, but this Wallaby build supports up to " +
                $"version {currentVersion}. A newer Wallaby has migrated this database; upgrade the package " +
                "instead of running an older build against it.");
        }
    }
}

/// <summary>Source-generated log messages for <see cref="StateSchemaBootstrapper"/>.</summary>
internal static partial class StateSchemaBootstrapperLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Migrated the wallaby state schema from version {FromVersion} to {ToVersion}.")]
    internal static partial void SchemaMigrated(this ILogger logger, long fromVersion, int toVersion);
}
