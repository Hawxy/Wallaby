using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// A fully decoded committed transaction: its changes in commit order, stamped with the commit LSN
/// and timestamp. This is the unit of acknowledgement for at-least-once delivery.
/// </summary>
internal sealed class CommittedTransaction
{
    /// <summary>LSN of the commit record (used for change metadata/ordering).</summary>
    public required ulong CommitLsn { get; init; }

    /// <summary>LSN just past the transaction; the value confirmed back to the server on acknowledgement.</summary>
    public required ulong EndLsn { get; init; }

    public DateTimeOffset? CommitTimestamp { get; init; }

    /// <summary>
    /// The in-memory changes in commit order (for a normal, non-streamed transaction). Empty when
    /// <see cref="IsStreamed"/> — read those changes via <see cref="Spill"/> instead.
    /// </summary>
    public required IReadOnlyList<RawChange> Changes { get; init; }

    /// <summary>
    /// Generic WAL messages (from <c>pg_logical_emit_message</c>) seen inside this transaction, in
    /// arrival order. Used by the backfill coordinator to bracket snapshot chunks with low/high
    /// watermarks; empty for ordinary data transactions (and always empty for streamed transactions, whose
    /// own tiny watermark transactions are never streamed).
    /// </summary>
    public IReadOnlyList<Watermark> Watermarks { get; init; } = Array.Empty<Watermark>();

    /// <summary>
    /// True when this transaction carried a <c>wallaby.heartbeat</c> message — an idle-slot heartbeat
    /// emitted only to advance <c>confirmed_flush_lsn</c>. Used to tag its span and keep it out of the
    /// throughput rollup; never set on streamed transactions (heartbeats are tiny).
    /// </summary>
    public bool ContainsHeartbeat { get; init; }

    /// <summary>
    /// True for a pgoutput v2 streamed (large) transaction whose changes were spilled out of memory rather
    /// than buffered in <see cref="Changes"/>. Read them in order via <c>Spill.ReadAsync(StreamXid)</c>; the
    /// consumer stamps each with this transaction's commit metadata and discards the spill when done.
    /// </summary>
    public bool IsStreamed { get; init; }

    /// <summary>The streamed transaction's xid (the spill key); meaningful only when <see cref="IsStreamed"/>.</summary>
    public uint StreamXid { get; init; }

    /// <summary>The spill holding a streamed transaction's changes; non-null only when <see cref="IsStreamed"/>.</summary>
    public ITransactionSpill? Spill { get; init; }
}

/// <summary>A decoded generic WAL message: prefix routes the consumer, token identifies the chunk.</summary>
internal readonly record struct Watermark(string Prefix, string Token);
