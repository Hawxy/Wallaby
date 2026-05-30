using EFCore.CDC.Model;

namespace EFCore.CDC.Internal.Replication;

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
    public required IReadOnlyList<RawChange> Changes { get; init; }

    /// <summary>
    /// Generic WAL messages (from <c>pg_logical_emit_message</c>) seen inside this transaction, in
    /// arrival order. Used by the backfill coordinator to bracket snapshot chunks with low/high
    /// watermarks; empty for ordinary data transactions.
    /// </summary>
    public IReadOnlyList<Watermark> Watermarks { get; init; } = Array.Empty<Watermark>();
}

/// <summary>A decoded generic WAL message: prefix routes the consumer, token identifies the chunk.</summary>
internal readonly record struct Watermark(string Prefix, string Token);
