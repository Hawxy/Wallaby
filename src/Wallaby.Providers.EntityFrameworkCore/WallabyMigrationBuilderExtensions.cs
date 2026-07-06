using Microsoft.EntityFrameworkCore.Migrations;

namespace Wallaby.Providers.EntityFrameworkCore;

/// <summary>Replica-identity DDL helpers for hand-written EF migrations.</summary>
public static class WallabyMigrationBuilderExtensions
{
    /// <summary>
    /// Set the table to <c>REPLICA IDENTITY FULL</c>, so old-row values ride the replication stream
    /// (needed by mappings with a scoped destination, and by transforms that project from the change's
    /// old values). Call in a migration's <c>Up()</c>; pair with
    /// <see cref="SetReplicaIdentityDefault"/> in <c>Down()</c>.
    /// </summary>
    public static void SetReplicaIdentityFull(
        this MigrationBuilder migrationBuilder, string table, string? schema = null)
        => SetReplicaIdentity(migrationBuilder, table, schema, "FULL");

    /// <summary>Restore the table to the default replica identity (primary key old values only).</summary>
    public static void SetReplicaIdentityDefault(
        this MigrationBuilder migrationBuilder, string table, string? schema = null)
        => SetReplicaIdentity(migrationBuilder, table, schema, "DEFAULT");

    private static void SetReplicaIdentity(
        MigrationBuilder migrationBuilder, string table, string? schema, string identity)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        var qualified = schema is null ? Quote(table) : $"{Quote(schema)}.{Quote(table)}";
        migrationBuilder.Sql($"ALTER TABLE {qualified} REPLICA IDENTITY {identity};");
    }

    private static string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";
}
