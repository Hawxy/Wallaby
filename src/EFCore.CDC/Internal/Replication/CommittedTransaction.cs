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
}
