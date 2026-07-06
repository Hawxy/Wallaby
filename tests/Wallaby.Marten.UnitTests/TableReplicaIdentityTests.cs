using Wallaby.Marten.Internal;
using Weasel.Postgresql;

namespace Wallaby.Marten.UnitTests;

/// <summary>The DDL a <see cref="TableReplicaIdentity"/> contributes to a Marten migration.</summary>
public class TableReplicaIdentityTests
{
    private static string Write(Action<TableReplicaIdentity, StringWriter> write)
    {
        var identity = new TableReplicaIdentity("docs", "mt_doc_softwidget");
        using var writer = new StringWriter();
        write(identity, writer);
        return writer.ToString();
    }

    [Test]
    public void Create_sets_full_replica_identity()
        => Write((i, w) => i.WriteCreateStatement(new PostgresqlMigrator(), w))
            .ShouldBe("ALTER TABLE \"docs\".\"mt_doc_softwidget\" REPLICA IDENTITY FULL;" + Environment.NewLine);

    [Test]
    public void Drop_restores_the_default_replica_identity()
        => Write((i, w) => i.WriteDropStatement(new PostgresqlMigrator(), w))
            .ShouldBe("ALTER TABLE \"docs\".\"mt_doc_softwidget\" REPLICA IDENTITY DEFAULT;" + Environment.NewLine);

    [Test]
    public void Identifier_does_not_collide_with_the_document_table()
        => Write((i, _) => i.Identifier.Name.ShouldBe("mt_doc_softwidget_replica_identity"));
}
