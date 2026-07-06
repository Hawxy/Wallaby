using Wallaby.Abstractions;

namespace Wallaby.Diagnostics;

/// <summary>
/// Mutable, thread-safe implementation of <see cref="IWallabyStatus"/>. The runtime, pipeline, and background
/// service update it at lifecycle points; reads (from health-check probe threads) are lock-free via an
/// atomically-swapped immutable snapshot.
/// </summary>
internal sealed class WallabyStatus : IWallabyStatus
{
    private readonly Lock _gate = new();
    private WallabyStatusSnapshot _snapshot;

    public WallabyStatus(string slotName = "", TimeProvider? clock = null)
    {
        _snapshot = new WallabyStatusSnapshot
        {
            Role = WallabyNodeRole.Starting,
            StartedAt = (clock ?? TimeProvider.System).GetUtcNow(),
            SlotName = slotName,
        };
    }

    public WallabyStatusSnapshot Current => Volatile.Read(ref _snapshot);

    // Mutations are rare (role transitions, once per committed transaction) so a lock is fine; readers stay
    // lock-free via Volatile.Read of the swapped immutable snapshot.
    private void Update(Func<WallabyStatusSnapshot, WallabyStatusSnapshot> mutate)
    {
        lock (_gate)
        {
            Volatile.Write(ref _snapshot, mutate(_snapshot));
        }
    }

    internal void EnterLeader(DateTimeOffset since) =>
        Update(s => s with { Role = WallabyNodeRole.Leader, LeaderSince = since, Faulted = false });

    internal void EnterStandby() =>
        Update(s => s with { Role = WallabyNodeRole.Standby, LeaderSince = null });

    internal void RecordLeaderFailure(string error) =>
        Update(s => s with { ConsecutiveLeaderFailures = s.ConsecutiveLeaderFailures + 1, LastError = error });

    internal void ResetLeaderFailures() =>
        Update(s => s.ConsecutiveLeaderFailures == 0 ? s : s with { ConsecutiveLeaderFailures = 0 });

    internal void RecordFanoutFailure(string error) =>
        Update(s => s with { ConsecutiveFanoutFailures = s.ConsecutiveFanoutFailures + 1, LastError = error });

    internal void ResetFanoutFailures() =>
        Update(s => s.ConsecutiveFanoutFailures == 0 ? s : s with { ConsecutiveFanoutFailures = 0 });

    internal void RecordProgress(ulong lsn, double lagSeconds, DateTimeOffset at) =>
        Update(s => s with
        {
            LastAcknowledgedLsn = lsn,
            LastProgressAt = at,
            // Keep the previous known lag when this transaction had no commit timestamp.
            LastIngestionLagSeconds = lagSeconds >= 0 ? lagSeconds : s.LastIngestionLagSeconds,
            // A fully delivered + acknowledged transaction proves the leader is healthy; a crash-looping
            // leader never gets here, so its failure count accumulates across sessions.
            ConsecutiveLeaderFailures = 0,
        });

    internal void RecordSinkDelivered(string sink, DateTimeOffset at) =>
        Update(s => s with
        {
            LastSinkDeliveryAt =
                new Dictionary<string, DateTimeOffset>(s.LastSinkDeliveryAt, StringComparer.Ordinal) { [sink] = at },
        });

    internal void MarkFaulted(string error) =>
        Update(s => s with { Role = WallabyNodeRole.Stopped, Faulted = true, LastError = error });

    internal void MarkStopped() =>
        Update(s => s with { Role = WallabyNodeRole.Stopped });
}
