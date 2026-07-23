using System.Threading.Channels;
using Wallaby.Internal;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.Replication;
using Wallaby.Model;

namespace Wallaby.Tests.Unit;

/// <summary>
/// The batcher against a scripted stream (an unbounded channel: buffered items complete reads
/// synchronously, an empty channel pends; the same completion shapes the replication enumerator has).
/// </summary>
public class TransactionBatcherTests
{
    private sealed class ScriptedStream
    {
        private readonly Channel<CommittedTransaction> _channel = Channel.CreateUnbounded<CommittedTransaction>();

        public void Add(params CommittedTransaction[] transactions)
        {
            foreach (var t in transactions)
            {
                _channel.Writer.TryWrite(t);
            }
        }

        public void Complete(Exception? error = null) => _channel.Writer.TryComplete(error);

        public IAsyncEnumerable<CommittedTransaction> ReadAllAsync(CancellationToken ct)
            => _channel.Reader.ReadAllAsync(ct);

        public TransactionBatcher Batcher(int maxTransactions = 100, int maxChanges = 1000)
            => new(ReadAllAsync(CancellationToken.None), maxTransactions, maxChanges, CancellationToken.None);
    }

    private static CommittedTransaction Txn(
        ulong lsn, int changes = 1, bool streamed = false, bool watermark = false, bool heartbeat = false)
        => new()
        {
            CommitLsn = lsn,
            EndLsn = lsn + 1,
            Changes = [.. Enumerable.Range(0, changes).Select(_ => new RawChange
            {
                RelationId = 1, Schema = "public", TableName = "t", Action = Abstractions.ChangeAction.Insert,
                NewValues = [],
            })],
            IsStreamed = streamed,
            StreamXid = streamed ? 7u : 0u,
            Watermarks = watermark ? [new Watermark(WallabySchema.WatermarkLowPrefix, "tok")] : [],
            ContainsHeartbeat = heartbeat,
        };

    [Test]
    public async Task Buffered_transactions_drain_into_one_batch()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10), Txn(20), Txn(30), Txn(40), Txn(50));
        await using var batcher = stream.Batcher();

        var batch = await batcher.ReadBatchAsync();

        batch!.Count.ShouldBe(5);
        batch.Select(t => t.CommitLsn).ShouldBe([10ul, 20ul, 30ul, 40ul, 50ul]);
        batcher.ReadInFlight.ShouldBeTrue(); // the drain left a pending read on the now-empty stream
    }

    [Test]
    public async Task An_idle_stream_yields_batches_of_one_and_resumes_from_the_pending_read()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10));
        await using var batcher = stream.Batcher();

        (await batcher.ReadBatchAsync())!.Single().CommitLsn.ShouldBe(10ul);

        stream.Add(Txn(20));
        (await batcher.ReadBatchAsync())!.Single().CommitLsn.ShouldBe(20ul);
    }

    [Test]
    public async Task The_transaction_cap_bounds_a_batch()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10), Txn(20), Txn(30), Txn(40), Txn(50));
        await using var batcher = stream.Batcher(maxTransactions: 3);

        (await batcher.ReadBatchAsync())!.Count.ShouldBe(3);
        (await batcher.ReadBatchAsync())!.Count.ShouldBe(2);
    }

    [Test]
    public async Task The_record_cap_bounds_a_batch()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10, changes: 4), Txn(20, changes: 4), Txn(30, changes: 4), Txn(40, changes: 4));
        await using var batcher = stream.Batcher(maxChanges: 10);

        // Add-then-check: the batch may overshoot the cap by at most one transaction.
        (await batcher.ReadBatchAsync())!.Count.ShouldBe(3);
        (await batcher.ReadBatchAsync())!.Count.ShouldBe(1);
    }

    [Test]
    public async Task Max_transactions_of_one_disables_coalescing()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10), Txn(20));
        await using var batcher = stream.Batcher(maxTransactions: 1);

        (await batcher.ReadBatchAsync())!.Single().CommitLsn.ShouldBe(10ul);
        (await batcher.ReadBatchAsync())!.Single().CommitLsn.ShouldBe(20ul);
    }

    [Test]
    public async Task A_streamed_transaction_is_a_solo_batch()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10), Txn(20, streamed: true, changes: 0), Txn(30));
        await using var batcher = stream.Batcher();

        (await batcher.ReadBatchAsync())!.Single().CommitLsn.ShouldBe(10ul);
        var solo = (await batcher.ReadBatchAsync())!.Single();
        solo.CommitLsn.ShouldBe(20ul);
        solo.IsStreamed.ShouldBeTrue();
        (await batcher.ReadBatchAsync())!.Single().CommitLsn.ShouldBe(30ul);
    }

    [Test]
    public async Task A_watermark_transaction_is_a_solo_batch()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10), Txn(20), Txn(30, watermark: true, changes: 0), Txn(40));
        await using var batcher = stream.Batcher();

        (await batcher.ReadBatchAsync())!.Count.ShouldBe(2);
        (await batcher.ReadBatchAsync())!.Single().Watermarks.ShouldNotBeEmpty();
        (await batcher.ReadBatchAsync())!.Single().CommitLsn.ShouldBe(40ul);
    }

    [Test]
    public async Task Heartbeat_transactions_coalesce_like_any_other()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10), Txn(20, heartbeat: true, changes: 0), Txn(30));
        await using var batcher = stream.Batcher();

        (await batcher.ReadBatchAsync())!.Count.ShouldBe(3);
    }

    [Test]
    public async Task End_of_stream_returns_the_final_batch_then_null()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10), Txn(20));
        stream.Complete();
        await using var batcher = stream.Batcher();

        (await batcher.ReadBatchAsync())!.Count.ShouldBe(2);
        (await batcher.ReadBatchAsync()).ShouldBeNull();
    }

    [Test]
    public async Task A_mid_drain_fault_is_delivered_after_the_current_batch()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10), Txn(20));
        stream.Complete(new InvalidOperationException("boom"));
        await using var batcher = stream.Batcher();

        // The transactions read before the fault deliver (and ack) first; the fault surfaces next.
        (await batcher.ReadBatchAsync())!.Count.ShouldBe(2);
        (await Should.ThrowAsync<InvalidOperationException>(() => batcher.ReadBatchAsync()))
            .Message.ShouldBe("boom");
    }

    [Test]
    public async Task Dispose_with_a_pending_read_does_not_hang()
    {
        var stream = new ScriptedStream();
        stream.Add(Txn(10));
        var batcher = stream.Batcher();
        await batcher.ReadBatchAsync();
        batcher.ReadInFlight.ShouldBeTrue();

        await batcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
    }
}
