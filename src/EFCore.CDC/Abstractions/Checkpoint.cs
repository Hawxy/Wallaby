namespace EFCore.CDC.Abstractions;

/// <summary>
/// The durable replication position for a slot: the highest commit LSN whose changes have been
/// delivered to all sinks. The slot's <c>confirmed_flush_lsn</c> is advanced to this value.
/// </summary>
/// <param name="ConfirmedLsn">The confirmed commit LSN, as a <see cref="ulong"/>.</param>
/// <param name="UpdatedAt">When the checkpoint was last persisted.</param>
public sealed record Checkpoint(ulong ConfirmedLsn, DateTimeOffset UpdatedAt);
