using Wallaby.Internal.Replication;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Accumulates committed transactions into bounded batches by greedy drain: after awaiting the first
/// transaction, more are added only while the stream's <c>MoveNextAsync</c> completes synchronously
/// (messages already buffered, the behind-the-stream case). An idle stream therefore yields batches of
/// one with no added latency, while a burst fills batches to the caps. Streamed and watermark-carrying
/// transactions are always solo batches: streamed transactions page through their spill, and watermark
/// ordering must not cross a batch boundary. A read left pending when a batch flushes stays in flight
/// (<see cref="ReadInFlight"/> lets the keepalive guard skip sends while Npgsql is reading) and is
/// resumed for the next batch. Transactions are never split across batches, so the last transaction's
/// <c>EndLsn</c> is the batch's acknowledgement point.
/// </summary>
internal sealed class TransactionBatcher : IAsyncDisposable
{
    private readonly IAsyncEnumerator<CommittedTransaction> _enumerator;
    private readonly CancellationTokenSource _cts;
    private readonly int _maxTransactions;
    private readonly int _maxChanges;

    private Task<bool>? _pending;
    private CommittedTransaction? _carried;
    private bool _ended;

    public TransactionBatcher(
        IAsyncEnumerable<CommittedTransaction> source, int maxTransactions, int maxChanges, CancellationToken ct)
    {
        // Linked so disposal can cancel a pending read (an idle stream may otherwise never complete it).
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _enumerator = source.GetAsyncEnumerator(_cts.Token);
        _maxTransactions = maxTransactions;
        _maxChanges = maxChanges;
    }

    /// <summary>Whether a stream read is in flight (Npgsql is answering the server's keepalives).</summary>
    public bool ReadInFlight => _pending is { IsCompleted: false };

    /// <summary>Read the next batch in stream order, or null at end of stream.</summary>
    public async Task<IReadOnlyList<CommittedTransaction>?> ReadBatchAsync()
    {
        CommittedTransaction first;
        if (_carried is not null)
        {
            first = _carried;
            _carried = null;
        }
        else
        {
            if (_ended || !await MoveNextAsync())
            {
                _ended = true;
                return null;
            }
            first = _enumerator.Current;
        }

        if (_maxTransactions == 1 || IsBoundary(first))
        {
            return [first];
        }

        List<CommittedTransaction> batch = [first];
        var changes = first.Changes.Count;

        while (batch.Count < _maxTransactions && changes < _maxChanges)
        {
            var read = _enumerator.MoveNextAsync();
            if (!read.IsCompletedSuccessfully)
            {
                // Pending (stream idle) or faulted: stop here. A fault is stashed and surfaces on the
                // next call, after the transactions already read have been delivered and acknowledged.
                _pending = read.AsTask();
                break;
            }
            if (!read.Result)
            {
                _ended = true;
                break;
            }

            var next = _enumerator.Current;
            if (IsBoundary(next))
            {
                _carried = next;
                break;
            }
            batch.Add(next);
            changes += next.Changes.Count;
        }

        return batch;
    }

    private async Task<bool> MoveNextAsync()
    {
        if (_pending is { } pending)
        {
            _pending = null;
            return await pending;
        }
        return await _enumerator.MoveNextAsync();
    }

    private static bool IsBoundary(CommittedTransaction transaction)
        => transaction.IsStreamed || transaction.Watermarks.Count > 0;

    public async ValueTask DisposeAsync()
    {
        // The enumerator contract forbids disposal with a read in flight; cancel and observe it first.
        await _cts.CancelAsync();
        if (_pending is { } pending)
        {
            _pending = null;
            try
            {
                await pending;
            }
            catch
            {
                // Unwind is already in progress; the pipeline surfaced (or is surfacing) its own reason.
            }
        }
        await _enumerator.DisposeAsync();
        _cts.Dispose();
    }
}
