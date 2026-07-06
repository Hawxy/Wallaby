using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Wallaby.TestInfrastructure.EntityFrameworkCore;

namespace Wallaby.Providers.EntityFrameworkCore.IntegrationTests;

/// <summary>
/// The <c>SetReplicaIdentity*</c> migration helpers against a real database: the emitted DDL flips
/// <c>pg_class.relreplident</c> both ways on a test-model table.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class ReplicaIdentityMigrationTests(TestModelPostgresFixture pg)
{
    [Test]
    public async Task Helper_ddl_sets_and_reverts_full_replica_identity()
    {
        await ExecuteHelperSqlAsync(b => b.SetReplicaIdentityFull("products", "public"));
        (await ReplicaIdentityAsync("products")).ShouldBe('f');

        await ExecuteHelperSqlAsync(b => b.SetReplicaIdentityDefault("products", "public"));
        (await ReplicaIdentityAsync("products")).ShouldBe('d');
    }

    /// <summary>Run the helper's generated SQL the way a migration would.</summary>
    private async Task ExecuteHelperSqlAsync(Action<MigrationBuilder> build)
    {
        var builder = new MigrationBuilder("Npgsql.EntityFrameworkCore.PostgreSQL");
        build(builder);
        var sql = builder.Operations.OfType<SqlOperation>().Single().Sql;

        await using var connection = await pg.DataSource.OpenConnectionAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<char> ReplicaIdentityAsync(string table)
    {
        await using var connection = await pg.DataSource.OpenConnectionAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "select c.relreplident from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
            "where n.nspname = 'public' and c.relname = $1";
        cmd.Parameters.AddWithValue(table);
        return (char)(await cmd.ExecuteScalarAsync())!;
    }
}
