using System.Data.Common;
using Wallaby.Internal;
using Weasel.Core;
using Weasel.Postgresql;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Wallaby.Marten.Internal;

/// <summary>
/// Weasel schema object that keeps one captured document table on <c>REPLICA IDENTITY FULL</c>. The
/// delta check reads <c>pg_class.relreplident</c>, so an already-converted table produces no statement;
/// a missing table also patches (its <c>CREATE TABLE</c> lands earlier in the same migration via the
/// feature's dependent types).
/// </summary>
internal sealed class TableReplicaIdentity(string schema, string table) : ISchemaObject
{
    public string Schema { get; } = schema;
    public string Table { get; } = table;

    // Logical name: the ALTER targets the document table, but the identifier must not collide with
    // Marten's own schema object for that table.
    public DbObjectName Identifier
        => new PostgresqlObjectName(Schema, $"{Table}_replica_identity", SchemaUtils.IdentifierUsage.General);

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
        => writer.WriteLine($"ALTER TABLE {PgExec.QuoteTable(Schema, Table)} REPLICA IDENTITY FULL;");

    public void WriteDropStatement(Migrator rules, TextWriter writer)
        => writer.WriteLine($"ALTER TABLE {PgExec.QuoteTable(Schema, Table)} REPLICA IDENTITY DEFAULT;");

    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        builder.Append(
            "select c.relreplident from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = ");
        builder.AppendParameter(Schema);
        builder.Append(" and c.relname = ");
        builder.AppendParameter(Table);
        builder.Append(";");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var full = await reader.ReadAsync(ct).ConfigureAwait(false) && reader.GetChar(0) == 'f';
        return new SchemaObjectDelta(this, full ? SchemaPatchDifference.None : SchemaPatchDifference.Create);
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }
}
