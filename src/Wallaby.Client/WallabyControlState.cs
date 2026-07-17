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
public sealed record WallabyManagedSlot(string SlotName, string Publication, string Kind, bool ExistsOnServer, bool Active);

/// <summary>A point-in-time view of the Wallaby control plane, read from the shared Postgres database.</summary>
/// <param name="State">The installation-wide suspension state.</param>
/// <param name="Origin">Who initiated the current (or most recent) suspension.</param>
/// <param name="Reason">The reason supplied with the suspension request, if any.</param>
/// <param name="RequestedBy">Who requested the suspension (defaults to the requesting machine name).</param>
/// <param name="RequestedAt">When the suspension was requested.</param>
/// <param name="SuspendedAt">When the suspension finalized (all managed slots verified dropped).</param>
/// <param name="ResumedAt">When the most recent resume happened.</param>
/// <param name="Slots">Every slot Wallaby manages, with live server state — while suspended, none should exist on the server.</param>
public sealed record WallabyControlState(
    WallabySuspensionState State,
    WallabySuspensionOrigin Origin,
    string? Reason,
    string? RequestedBy,
    DateTimeOffset? RequestedAt,
    DateTimeOffset? SuspendedAt,
    DateTimeOffset? ResumedAt,
    IReadOnlyList<WallabyManagedSlot> Slots);
