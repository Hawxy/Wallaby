using System.Collections.Concurrent;
using Wallaby.Abstractions;

namespace Wallaby.Diagnostics;

/// <summary>
/// Mutable, thread-safe implementation of <see cref="IWallabyStatus"/>. The runtime, pipeline, and background
/// service update it at lifecycle points; reads (from health-check probe threads) are lock-free via an
/// atomically-swapped immutable snapshot. Per-sink delivery timestamps are the highest-frequency update, so
/// they mutate a concurrent map in place and are composed into the snapshot on read — not atomic with the
/// other snapshot fields.
/// </summary>
internal sealed class WallabyStatus : IWallabyStatus
{
    private readonly Lock _gate = new();
    private readonly ConcurrentDictionary<string, DateTimeOffset> _sinkDeliveries = new(StringComparer.Ordinal);
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

    public WallabyStatusSnapshot Current
    {
        get
        {
            var snapshot = Volatile.Read(ref _snapshot);
            return _sinkDeliveries.IsEmpty
                ? snapshot
                : snapshot with
                {
                    LastSinkDeliveryAt = new Dictionary<string, DateTimeOffset>(_sinkDeliveries, StringComparer.Ordinal),
                };
        }
    }

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
        Update(s => s with
        {
            Role = WallabyNodeRole.Leader, LeaderSince = since, Faulted = false,
            SuspendedSince = null, SuspensionReason = null,
        });

    internal void EnterStandby() =>
        Update(s => s with
        {
            Role = WallabyNodeRole.Standby, LeaderSince = null,
            SuspendedSince = null, SuspensionReason = null,
        });

    internal void EnterSuspended(DateTimeOffset? since, string? reason) =>
        Update(s => s with
        {
            Role = WallabyNodeRole.Suspended, LeaderSince = null,
            SuspendedSince = since, SuspensionReason = reason,
        });

    internal void SetPublicationsWidened(bool widened, DateTimeOffset? at) =>
        Update(s => s.PublicationsWidened == widened && s.PublicationsWidenedAt == at
            ? s
            : s with { PublicationsWidened = widened, PublicationsWidenedAt = at });

    internal void RecordLeaderFailure(string error) =>
        Update(s => s with { ConsecutiveLeaderFailures = s.ConsecutiveLeaderFailures + 1, LastError = error });

    internal void ResetLeaderFailures() =>
        Update(s => s.ConsecutiveLeaderFailures == 0 ? s : s with { ConsecutiveLeaderFailures = 0 });

    internal void RecordFanoutFailure(string error) =>
        Update(s => s with { ConsecutiveFanoutFailures = s.ConsecutiveFanoutFailures + 1, LastError = error });

    /// <summary>A fan-out job failed; <paramref name="attempts"/> is its persisted failure streak.</summary>
    internal void RecordFanoutJobFailure(string error, int attempts) =>
        Update(s => s with
        {
            ConsecutiveFanoutFailures = Math.Max(s.ConsecutiveFanoutFailures, attempts),
            LastError = error,
        });

    /// <summary>
    /// Reconcile the counter with the store's worst pending job. Persisted attempts are the source of
    /// truth: healthy jobs draining can't mask a backed-off failing job, and a recovered job's deleted
    /// row lowers the value on its own.
    /// </summary>
    internal void SetFanoutStreak(int attempts) =>
        Update(s => s.ConsecutiveFanoutFailures == attempts ? s : s with { ConsecutiveFanoutFailures = attempts });

    internal void ResetFanoutFailures() => SetFanoutStreak(0);

    /// <summary>A scheduler pass failed outright (the store unreachable, not one table's run).</summary>
    internal void RecordBackfillFailure(string error) =>
        Update(s => s with { ConsecutiveBackfillFailures = s.ConsecutiveBackfillFailures + 1, LastError = error });

    /// <summary>A table's backfill failed; <paramref name="attempts"/> is its persisted failure streak.</summary>
    internal void RecordBackfillTableFailure(string error, int attempts) =>
        Update(s => s with
        {
            ConsecutiveBackfillFailures = Math.Max(s.ConsecutiveBackfillFailures, attempts),
            LastError = error,
        });

    /// <summary>
    /// Reconcile the counter with the store's worst pending table. Persisted attempts are the source of
    /// truth: healthy tables running can't mask a backed-off failing one, and a recovered table's reset
    /// ledger lowers the value on its own.
    /// </summary>
    internal void SetBackfillStreak(int attempts) =>
        Update(s => s.ConsecutiveBackfillFailures == attempts ? s : s with { ConsecutiveBackfillFailures = attempts });

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

    internal void RecordSinkDelivered(string sink, DateTimeOffset at) => _sinkDeliveries[sink] = at;

    internal void MarkFaulted(string error) =>
        Update(s => s with { Role = WallabyNodeRole.Stopped, Faulted = true, LastError = error });

    internal void MarkStopped() =>
        Update(s => s with { Role = WallabyNodeRole.Stopped });
}
