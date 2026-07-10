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

    private async Task<PostgresUnloggedTableSpill> CreateSpillAsync()
    {
        await using (var conn = await pg.DataSource.OpenConnectionAsync())
        {
            await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
        }
        return new PostgresUnloggedTableSpill(pg.DataSource, "spilltest_" + Guid.NewGuid().ToString("N"));
    }

    [Test]
    public async Task Unlogged_table_spill_round_trips_and_clears()
    {
        var spill = await CreateSpillAsync();
        try
        {
            const int n = 1200; // > FlushThreshold (500) so multiple COPY flushes occur
            for (var i = 0; i < n; i++)
            {
                await spill.AppendAsync(7, 7, Change(i), CancellationToken.None);
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

    [Test]
    public async Task Subtransaction_discard_deletes_flushed_rows_from_first_appearance()
    {
        var spill = await CreateSpillAsync();
        try
        {
            // 600 toplevel + 600 subtransaction changes: both sides cross the 500 FlushThreshold.
            for (var i = 0; i < 600; i++)
            {
                await spill.AppendAsync(7, 7, Change(i), CancellationToken.None);
            }
            for (var i = 600; i < 1200; i++)
            {
                await spill.AppendAsync(7, 100, Change(i), CancellationToken.None);
            }

            await spill.DiscardSubtransactionAsync(7, 100, CancellationToken.None);

            var read = await ReadAllAsync(spill, 7);
            read.Count.ShouldBe(600);
            ((int)read[^1].NewValues[0].Value!).ShouldBe(599);
        }
        finally
        {
            await spill.ClearAsync(CancellationToken.None);
            await spill.DisposeAsync();
        }
    }

    [Test]
    public async Task Subtransaction_discard_covering_flushed_and_pending_rows()
    {
        var spill = await CreateSpillAsync();
        try
        {
            // 499 toplevel + 1 subtransaction change hit the threshold together, so the subxid's first
            // change is flushed; 3 more stay pending. The delete must clear the pending tail too.
            for (var i = 0; i < 499; i++)
            {
                await spill.AppendAsync(7, 7, Change(i), CancellationToken.None);
            }
            for (var i = 499; i < 503; i++)
            {
                await spill.AppendAsync(7, 100, Change(i), CancellationToken.None);
            }

            await spill.DiscardSubtransactionAsync(7, 100, CancellationToken.None);

            (await ReadAllAsync(spill, 7)).Count.ShouldBe(499);
        }
        finally
        {
            await spill.ClearAsync(CancellationToken.None);
            await spill.DisposeAsync();
        }
    }

    [Test]
    public async Task Subtransaction_discard_of_pending_only_rows()
    {
        var spill = await CreateSpillAsync();
        try
        {
            await spill.AppendAsync(7, 7, Change(0), CancellationToken.None);
            await spill.AppendAsync(7, 7, Change(1), CancellationToken.None);
            await spill.AppendAsync(7, 7, Change(2), CancellationToken.None);
            await spill.AppendAsync(7, 100, Change(3), CancellationToken.None);
            await spill.AppendAsync(7, 100, Change(4), CancellationToken.None);

            await spill.DiscardSubtransactionAsync(7, 100, CancellationToken.None);

            (await ReadAllAsync(spill, 7)).Count.ShouldBe(3);
        }
        finally
        {
            await spill.ClearAsync(CancellationToken.None);
            await spill.DisposeAsync();
        }
    }

    [Test]
    public async Task Subtransaction_discard_noop_and_later_appends_survive()
    {
        var spill = await CreateSpillAsync();
        try
        {
            await spill.AppendAsync(7, 7, Change(0), CancellationToken.None);
            await spill.AppendAsync(7, 100, Change(1), CancellationToken.None);

            await spill.DiscardSubtransactionAsync(7, 999, CancellationToken.None);  // unseen subxid
            (await ReadAllAsync(spill, 7)).Count.ShouldBe(2);

            await spill.DiscardSubtransactionAsync(7, 100, CancellationToken.None);
            await spill.AppendAsync(7, 7, Change(2), CancellationToken.None);        // post-rollback change

            var read = await ReadAllAsync(spill, 7);
            read.Count.ShouldBe(2);
            ((int)read[^1].NewValues[0].Value!).ShouldBe(2);
        }
        finally
        {
            await spill.ClearAsync(CancellationToken.None);
            await spill.DisposeAsync();
        }
    }
}
