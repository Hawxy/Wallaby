using Wallaby.Abstractions;

namespace Wallaby.Diagnostics;

/// <summary>
/// Mutable, thread-safe implementation of <see cref="ICdcStatus"/>. The runtime, pipeline, and background
/// service update it at lifecycle points; reads (from health-check probe threads) are lock-free via an
/// atomically-swapped immutable snapshot.
/// </summary>
internal sealed class CdcStatus : ICdcStatus
{
    private readonly Lock _gate = new();
    private CdcStatusSnapshot _snapshot;

    public CdcStatus(string slotName = "", TimeProvider? clock = null)
    {
        _snapshot = new CdcStatusSnapshot
        {
            Role = CdcNodeRole.Starting,
            StartedAt = (clock ?? TimeProvider.System).GetUtcNow(),
            SlotName = slotName,
        };
    }

    public CdcStatusSnapshot Current => Volatile.Read(ref _snapshot);

    // Mutations are rare (role transitions, once per committed transaction) so a lock is fine; readers stay
    // lock-free via Volatile.Read of the swapped immutable snapshot.
    private void Update(Func<CdcStatusSnapshot, CdcStatusSnapshot> mutate)
    {
        lock (_gate)
        {
            Volatile.Write(ref _snapshot, mutate(_snapshot));
        }
    }

    internal void EnterLeader(DateTimeOffset since) =>
        Update(s => s with { Role = CdcNodeRole.Leader, LeaderSince = since, Faulted = false });

    internal void EnterStandby() =>
        Update(s => s with { Role = CdcNodeRole.Standby, LeaderSince = null });

    internal void RecordLeaderFailure(string error) =>
        Update(s => s with { ConsecutiveLeaderFailures = s.ConsecutiveLeaderFailures + 1, LastError = error });

    internal void ResetLeaderFailures() =>
        Update(s => s.ConsecutiveLeaderFailures == 0 ? s : s with { ConsecutiveLeaderFailures = 0 });

    internal void RecordProgress(ulong lsn, double lagSeconds, DateTimeOffset at) =>
        Update(s => s with
        {
            LastAcknowledgedLsn = lsn,
            LastProgressAt = at,
            // Keep the previous known lag when this transaction had no commit timestamp.
            LastIngestionLagSeconds = lagSeconds >= 0 ? lagSeconds : s.LastIngestionLagSeconds,
        });

    internal void MarkFaulted(string error) =>
        Update(s => s with { Role = CdcNodeRole.Stopped, Faulted = true, LastError = error });

    internal void MarkStopped() =>
        Update(s => s with { Role = CdcNodeRole.Stopped });
}
