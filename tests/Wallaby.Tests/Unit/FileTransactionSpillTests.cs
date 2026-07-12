using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Model;

namespace Wallaby.Tests.Unit;

/// <summary><see cref="FileTransactionSpill"/> must append and read back a streamed transaction's changes in order.</summary>
public class FileTransactionSpillTests
{
    private static RawChange Change(int id, string name) => new()
    {
        RelationId = 1,
        Schema = "public",
        TableName = "products",
        Action = ChangeAction.Insert,
        NewValues = [new RawColumn { ColumnName = "id", Value = id }, new RawColumn { ColumnName = "name", Value = name }],
    };

    private static async Task<List<RawChange>> ReadAllAsync(ITransactionSpill spill, uint xid)
    {
        var read = new List<RawChange>();
        await foreach (var c in spill.ReadAsync(xid, default))
        {
            read.Add(c);
        }
        return read;
    }

    [Test]
    public async Task Appends_are_read_back_in_order()
    {
        var dir = Path.Combine(Path.GetTempPath(), "wallaby-spill-test", Guid.NewGuid().ToString("N"));
        var spill = new FileTransactionSpill(dir);
        try
        {
            for (var i = 0; i < 5; i++)
            {
                await spill.AppendAsync(42, 42, Change(i, $"p{i}"), default);
            }

            var read = await ReadAllAsync(spill, 42);

            read.Count.ShouldBe(5);
            string.Join(",", read.Select(c => c.NewValues[1].Value)).ShouldBe("p0,p1,p2,p3,p4");
        }
        finally
        {
            await spill.DisposeAsync();
            TryDelete(dir);
        }
    }

    [Test]
    public async Task Discard_and_isolation_between_xids()
    {
        var dir = Path.Combine(Path.GetTempPath(), "wallaby-spill-test", Guid.NewGuid().ToString("N"));
        var spill = new FileTransactionSpill(dir);
        try
        {
            await spill.AppendAsync(1, 1, Change(0, "a"), default);
            await spill.AppendAsync(2, 2, Change(0, "b"), default);

            (await ReadAllAsync(spill, 1)).Single().NewValues[1].Value.ShouldBe("a");
            (await ReadAllAsync(spill, 2)).Single().NewValues[1].Value.ShouldBe("b");

            await spill.DiscardAsync(1, default);
            (await ReadAllAsync(spill, 1)).Count.ShouldBe(0);
            (await ReadAllAsync(spill, 2)).Count.ShouldBe(1); // unaffected
        }
        finally
        {
            await spill.DisposeAsync();
            TryDelete(dir);
        }
    }

    [Test]
    public async Task Subtransaction_discard_truncates_from_its_first_change()
    {
        var dir = Path.Combine(Path.GetTempPath(), "wallaby-spill-test", Guid.NewGuid().ToString("N"));
        var spill = new FileTransactionSpill(dir);
        try
        {
            await spill.AppendAsync(42, 42, Change(0, "top0"), default);
            await spill.AppendAsync(42, 42, Change(1, "top1"), default);
            await spill.AppendAsync(42, 100, Change(2, "sub0"), default);
            await spill.AppendAsync(42, 100, Change(3, "sub1"), default);
            await spill.AppendAsync(42, 101, Change(4, "nested"), default); // nested inside 100

            await spill.DiscardSubtransactionAsync(42, 100, default);

            var read = await ReadAllAsync(spill, 42);
            string.Join(",", read.Select(c => c.NewValues[1].Value)).ShouldBe("top0,top1");
        }
        finally
        {
            await spill.DisposeAsync();
            TryDelete(dir);
        }
    }

    [Test]
    public async Task Subtransaction_discard_for_an_unseen_subxid_is_a_noop()
    {
        var dir = Path.Combine(Path.GetTempPath(), "wallaby-spill-test", Guid.NewGuid().ToString("N"));
        var spill = new FileTransactionSpill(dir);
        try
        {
            await spill.AppendAsync(42, 42, Change(0, "a"), default);
            await spill.AppendAsync(42, 42, Change(1, "b"), default);
            await spill.AppendAsync(42, 42, Change(2, "c"), default);

            await spill.DiscardSubtransactionAsync(42, 999, default);  // savepoint touched no published table
            await spill.DiscardSubtransactionAsync(7, 999, default);   // xid with no spill at all

            (await ReadAllAsync(spill, 42)).Count.ShouldBe(3);
            (await ReadAllAsync(spill, 7)).Count.ShouldBe(0);
        }
        finally
        {
            await spill.DisposeAsync();
            TryDelete(dir);
        }
    }

    [Test]
    public async Task Changes_appended_after_a_subtransaction_discard_survive()
    {
        var dir = Path.Combine(Path.GetTempPath(), "wallaby-spill-test", Guid.NewGuid().ToString("N"));
        var spill = new FileTransactionSpill(dir);
        try
        {
            await spill.AppendAsync(42, 42, Change(0, "t1"), default);
            await spill.AppendAsync(42, 100, Change(1, "gone1"), default);
            await spill.AppendAsync(42, 100, Change(2, "gone2"), default);
            await spill.DiscardSubtransactionAsync(42, 100, default);

            // A re-established savepoint after the rollback, itself rolled back too.
            await spill.AppendAsync(42, 101, Change(3, "gone3"), default);
            await spill.AppendAsync(42, 101, Change(4, "gone4"), default);
            await spill.DiscardSubtransactionAsync(42, 101, default);

            await spill.AppendAsync(42, 42, Change(5, "t2"), default);

            var read = await ReadAllAsync(spill, 42);
            string.Join(",", read.Select(c => c.NewValues[1].Value)).ShouldBe("t1,t2");
        }
        finally
        {
            await spill.DisposeAsync();
            TryDelete(dir);
        }
    }

    [Test]
    public async Task Subtransaction_discard_does_not_affect_other_xids()
    {
        var dir = Path.Combine(Path.GetTempPath(), "wallaby-spill-test", Guid.NewGuid().ToString("N"));
        var spill = new FileTransactionSpill(dir);
        try
        {
            await spill.AppendAsync(1, 1, Change(0, "a"), default);
            await spill.AppendAsync(1, 50, Change(1, "a-sub"), default);
            await spill.AppendAsync(2, 2, Change(2, "b"), default);
            await spill.AppendAsync(2, 60, Change(3, "b-sub"), default);

            await spill.DiscardSubtransactionAsync(1, 50, default);

            (await ReadAllAsync(spill, 1)).Count.ShouldBe(1);
            (await ReadAllAsync(spill, 2)).Count.ShouldBe(2); // unaffected
        }
        finally
        {
            await spill.DisposeAsync();
            TryDelete(dir);
        }
    }

    private static void TryDelete(string dir)
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
    }
}
