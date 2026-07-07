using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Internal.State;
using Wallaby.Model;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

/// <summary>
/// <see cref="PostgresUnloggedTableSpill"/> round-trip against a real database: append (across multiple COPY
/// flushes), read back in order, discard, clear. Uses a unique slot name to stay isolated in the shared DB.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class SpillStoreTests(PostgresFixture pg)
{
    private static RawChange Change(int id) => new()
    {
        RelationId = 1,
        Schema = "public",
        TableName = "products",
        Action = ChangeAction.Insert,
        NewValues = [new RawColumn { ColumnName = "id", Value = id }, new RawColumn { ColumnName = "name", Value = $"p{id}" }],
    };

    private static async Task<List<RawChange>> ReadAllAsync(ITransactionSpill spill, uint xid)
    {
        var read = new List<RawChange>();
        await foreach (var c in spill.ReadAsync(xid, CancellationToken.None))
        {
            read.Add(c);
        }
        return read;
    }

    [Test]
    public async Task Unlogged_table_spill_round_trips_and_clears()
    {
        await using (var conn = await pg.DataSource.OpenConnectionAsync())
        {
            await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
        }

        var slot = "spilltest_" + Guid.NewGuid().ToString("N");
        var spill = new PostgresUnloggedTableSpill(pg.DataSource, slot);
        try
        {
            const int n = 1200; // > FlushThreshold (500) so multiple COPY flushes occur
            for (var i = 0; i < n; i++)
            {
                await spill.AppendAsync(7, Change(i), CancellationToken.None);
            }

            var read = await ReadAllAsync(spill, 7);
            read.Count.ShouldBe(n);
            ((int)read[0].NewValues[0].Value!).ShouldBe(0);
            ((int)read[^1].NewValues[0].Value!).ShouldBe(n - 1);
            read[500].NewValues[1].Value.ShouldBe("p500"); // ordering preserved across flushes

            await spill.DiscardAsync(7, CancellationToken.None);
            (await ReadAllAsync(spill, 7)).Count.ShouldBe(0);
        }
        finally
        {
            await spill.ClearAsync(CancellationToken.None);
            await spill.DisposeAsync();
        }
    }
}
