using Wallaby.Abstractions;

namespace Wallaby.Internal.State;

/// <summary>
/// Rate-limits checkpoint writes to at most one per <paramref name="interval"/>. The checkpoint row is
/// supplementary (the slot's <c>confirmed_flush_lsn</c> is the authoritative resume position), and a
/// seconds-stale checkpoint only widens a detected slot-loss gap — the repair is a full re-backfill
/// either way — so skipping intermediate writes trades nothing for a per-transaction round trip saved.
/// The caller is the single-threaded pipeline loop.
/// </summary>
internal sealed class ThrottledCheckpointStore(
    ICheckpointStore inner, TimeSpan interval, TimeProvider? clock = null) : ICheckpointStore
{
    private readonly TimeProvider _clock = clock ?? TimeProvider.System;
    private long? _lastSaved;

    public Task<Checkpoint?> GetAsync(string slotName, CancellationToken ct) => inner.GetAsync(slotName, ct);

    public async Task SaveAsync(string slotName, Checkpoint checkpoint, CancellationToken ct)
    {
        var now = _clock.GetTimestamp();
        if (_lastSaved is { } last && _clock.GetElapsedTime(last, now) < interval)
        {
            return;
        }

        await inner.SaveAsync(slotName, checkpoint, ct);
        _lastSaved = now;
    }
}
