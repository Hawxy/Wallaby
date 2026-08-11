namespace Wallaby.Client;

/// <summary>The installation-wide suspension state of a Wallaby deployment.</summary>
public enum WallabySuspensionState
{
    /// <summary>Not suspended: Wallaby provisions and streams normally.</summary>
    Running,

    /// <summary>A suspension has been requested; managed slots are being (or about to be) dropped.</summary>
    SuspendRequested,

    /// <summary>Every managed replication slot has been dropped; Wallaby idles until an explicit resume.</summary>
    Suspended,
}

/// <summary>Who initiated the current (or most recent) suspension.</summary>
public enum WallabySuspensionOrigin
{
    /// <summary>Requested at runtime (e.g. via <see cref="WallabyControlClient.SuspendAsync"/>); persists until an explicit resume.</summary>
    Client,

    /// <summary>Asserted by a deployed <c>Suspend()</c> builder flag; auto-resumed by a node deployed without the flag.</summary>
    Configuration,
}

/// <summary>A replication slot Wallaby manages, joined with its live server state.</summary>
/// <param name="SlotName">The slot name.</param>
/// <param name="Publication">The publication the slot was provisioned for.</param>
/// <param name="Kind"><c>primary</c> (Wallaby's own capture slot) or <c>external</c> (provisioned for a third-party consumer).</param>
/// <param name="ExistsOnServer">Whether the slot currently exists in <c>pg_replication_slots</c>.</param>
/// <param name="Active">Whether a consumer is currently streaming from the slot.</param>
/// <param name="RetainedWalBytes">
/// WAL bytes the server retains for the slot (its <c>restart_lsn</c> to the current write position).
/// External slots pin WAL from the moment they exist, so watch this for slots whose consumer lags or
/// never connects. <c>null</c> when the slot doesn't exist on the server, or when reading from a
/// standby in recovery.
/// </param>
/// <param name="PublicationManaged">
/// Whether Wallaby owns <paramref name="Publication"/> (created it and recreates it from configuration).
/// Suspension drops managed publications alongside the slots; an unmanaged one
/// (<c>ManagePublicationTables=false</c>) is left untouched.
/// </param>
/// <param name="PublicationNarrowed">
/// Whether <paramref name="Publication"/> currently carries a column list or row filter on the server —
/// the condition that refuses <c>ALTER COLUMN ... TYPE</c> on the referenced columns. Live progress for
/// <see cref="WallabyControlClient.WidenPublicationsAsync"/>: widening completes when no managed
/// publication is narrowed.
/// </param>
public sealed record WallabyManagedSlot(
    string SlotName, string Publication, string Kind, bool ExistsOnServer, bool Active,
    long? RetainedWalBytes = null, bool PublicationManaged = false, bool PublicationNarrowed = false);

/// <summary>A point-in-time view of the Wallaby control plane, read from the shared Postgres database.</summary>
/// <param name="State">The installation-wide suspension state.</param>
/// <param name="Origin">Who initiated the current (or most recent) suspension.</param>
/// <param name="Reason">The reason supplied with the suspension request, if any.</param>
/// <param name="RequestedBy">Who requested the suspension (defaults to the requesting machine name).</param>
/// <param name="RequestedAt">When the suspension was requested.</param>
/// <param name="SuspendedAt">When the suspension finalized (all managed slots verified dropped).</param>
/// <param name="ResumedAt">When the most recent resume happened.</param>
/// <param name="Slots">Every slot Wallaby manages, with live server state — while suspended, none should exist on the server.</param>
/// <param name="PublicationsWidened">
/// True while managed publications are (or are being) widened to whole-table membership so schema
/// migrations blocked by publication column lists can run; cleared by
/// <see cref="WallabyControlClient.RestorePublicationsAsync"/>.
/// </param>
/// <param name="WidenedAt">When the current widening was requested.</param>
/// <param name="WidenedBy">Who requested the current widening.</param>
/// <param name="PurgeOnResume">
/// True while a <see cref="WallabyControlClient.ResumeAsync(bool, CancellationToken)"/>
/// purge request is pending: the next leader session's slot-gap repair purges sink destinations before
/// its re-backfills, then clears the flag. Discarded (with a host-side warning) if that session finds
/// no gap to repair.
/// </param>
public sealed record WallabyControlState(
    WallabySuspensionState State,
    WallabySuspensionOrigin Origin,
    string? Reason,
    string? RequestedBy,
    DateTimeOffset? RequestedAt,
    DateTimeOffset? SuspendedAt,
    DateTimeOffset? ResumedAt,
    IReadOnlyList<WallabyManagedSlot> Slots,
    bool PublicationsWidened = false,
    DateTimeOffset? WidenedAt = null,
    string? WidenedBy = null,
    bool PurgeOnResume = false);
