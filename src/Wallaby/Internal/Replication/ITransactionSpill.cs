using Npgsql;
using Wallaby.Model;

namespace Wallaby.Abstractions;

/// <summary>
/// Buffers an in-progress <b>streamed</b> (large) transaction's changes out of process memory until its commit,
/// so a single huge transaction doesn't exhaust the heap. Changes are appended per xid as they stream and read
/// back in append order at <c>StreamCommit</c>; an aborted or already-consumed xid is discarded, and a rolled-back
/// savepoint truncates just its subtransaction's changes (<see cref="DiscardSubtransactionAsync"/>). A streamed
/// transaction that never commits (a crash) is re-streamed from the slot, so the spill need not survive a
/// restart — <see cref="ClearAsync"/> drops any leftovers on startup. Implementations are used only from the
/// single-threaded replication loop (no concurrent calls for the same xid).
/// <para>
/// This is a pluggable extension point: Wallaby ships a disk backend (<c>SpillToDisk</c>) and a source-database
/// backend (<c>SpillToDatabase</c>, the default), and a custom backend can be supplied via
/// <c>UseTransactionSpill(...)</c>. An implementation owns its own serialization of <see cref="RawChange"/> —
/// the abstraction deals purely in changes. Only a store that spills to durable/external storage actually bounds
/// memory; an in-RAM store would merely relocate it.
/// </para>
/// </summary>
public interface ITransactionSpill : IAsyncDisposable
{
    /// <summary>
    /// Append one change to the buffer for <paramref name="xid"/> (preserving stream order).
    /// <paramref name="subxid"/> is the xid of the (sub)transaction that made the change — equal to
    /// <paramref name="xid"/> for changes made directly in the top-level transaction — and is what a later
    /// <see cref="DiscardSubtransactionAsync"/> truncates by.
    /// </summary>
    ValueTask AppendAsync(uint xid, uint subxid, RawChange change, CancellationToken ct);

    /// <summary>
    /// Read back, in append order, every change buffered for <paramref name="xid"/>. If the backing store no
    /// longer holds everything appended (external mutation), the read should fail rather than yield a partial
    /// buffer — a partial read that succeeded would be delivered and acknowledged as if complete.
    /// </summary>
    IAsyncEnumerable<RawChange> ReadAsync(uint xid, CancellationToken ct);

    /// <summary>Drop the buffer for <paramref name="xid"/> (after its commit is consumed, or on abort).</summary>
    ValueTask DiscardAsync(uint xid, CancellationToken ct);

    /// <summary>
    /// A subtransaction (savepoint) of the streamed transaction <paramref name="xid"/> rolled back: remove the
    /// changes appended with <paramref name="subxid"/> <b>and every change appended after its first one</b> — a
    /// change after that point can only belong to the aborted subtransaction or to a subtransaction nested inside
    /// it, which aborts with it (the same truncate-from-first-change semantics as Postgres's own
    /// logical-replication apply worker). Must be a no-op when <paramref name="subxid"/> never appended a change
    /// for <paramref name="xid"/> (the rolled-back savepoint touched no published table, or no spill exists at
    /// all). Changes appended for <paramref name="xid"/> after this call must survive and be returned by a later
    /// <see cref="ReadAsync"/>.
    /// </summary>
    ValueTask DiscardSubtransactionAsync(uint xid, uint subxid, CancellationToken ct);

    /// <summary>Drop all buffered data for this slot (startup cleanup; the slot re-streams anything un-acked).</summary>
    ValueTask ClearAsync(CancellationToken ct);
}

/// <summary>
/// What a <see cref="ITransactionSpill"/> factory is handed when the runtime builds the spill for a leader session
/// (see <c>UseTransactionSpill</c>). Carries the things a backend commonly needs: the pooled source
/// <see cref="DataSource"/> (for a Postgres-backed spill), the <see cref="SlotName"/> to namespace buffered data,
/// and the application <see cref="Services"/> (to resolve a custom backend's own dependencies, e.g. a cache client).
/// </summary>
/// <param name="DataSource">The pooled connection source for the configured Postgres database.</param>
/// <param name="SlotName">The replication slot name — use it to namespace this spill's buffered data.</param>
/// <param name="Services">The application service provider, for resolving a custom backend's dependencies.</param>
public readonly record struct SpillContext(NpgsqlDataSource DataSource, string SlotName, IServiceProvider Services);
