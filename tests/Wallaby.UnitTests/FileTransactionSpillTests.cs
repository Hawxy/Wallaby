using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Model;

namespace Wallaby.UnitTests;

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
                await spill.AppendAsync(42, Change(i, $"p{i}"), default);
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
            await spill.AppendAsync(1, Change(0, "a"), default);
            await spill.AppendAsync(2, Change(0, "b"), default);

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

    private static void TryDelete(string dir)
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
    }
}
