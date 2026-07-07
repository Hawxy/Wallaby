using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit;

/// <summary>The DDL the <c>SetReplicaIdentity*</c> migration helpers add to a migration.</summary>
public class ReplicaIdentityMigrationTests
{
    private static string SingleSql(Action<MigrationBuilder> build)
    {
        var builder = new MigrationBuilder("Npgsql.EntityFrameworkCore.PostgreSQL");
        build(builder);
        return builder.Operations.OfType<SqlOperation>().ShouldHaveSingleItem().Sql;
    }

    [Test]
    public void Full_emits_the_alter_with_schema_qualification()
        => SingleSql(b => b.SetReplicaIdentityFull("orders", "sales"))
            .ShouldBe("ALTER TABLE \"sales\".\"orders\" REPLICA IDENTITY FULL;");

    [Test]
    public void Full_omits_the_schema_when_not_given()
        => SingleSql(b => b.SetReplicaIdentityFull("orders"))
            .ShouldBe("ALTER TABLE \"orders\" REPLICA IDENTITY FULL;");

    [Test]
    public void Default_emits_the_reverting_alter()
        => SingleSql(b => b.SetReplicaIdentityDefault("orders", "sales"))
            .ShouldBe("ALTER TABLE \"sales\".\"orders\" REPLICA IDENTITY DEFAULT;");
}
