namespace Wallaby.Client.Internal;

/// <summary>
/// The shared contract for the <c>wallaby.backfill_state</c> table and its notify channel, compile-linked
/// into both Wallaby (the host side) and Wallaby.Client (the remote client) like
/// <see cref="ControlContract"/>. The table is created only by the host; the client never performs DDL.
/// Status strings mirror the host's <c>BackfillStatus</c> enum names.
/// </summary>
internal static class BackfillContract
{
    public const string Table = "wallaby.backfill_state";

    /// <summary>LISTEN/NOTIFY channel signalled when a backfill request is persisted.</summary>
    public const string NotifyChannel = "wallaby_backfill";

    public const string StatusRequested = "Requested";
}
