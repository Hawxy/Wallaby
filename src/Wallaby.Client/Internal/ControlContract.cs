namespace Wallaby.Client.Internal;

/// <summary>
/// The shared contract for the <c>wallaby.control</c> table and its notify channel. Compile-linked into
/// both Wallaby (the host side) and Wallaby.Client (the remote client) so the two agree on the
/// wire format without a package reference in either direction. The table itself is created only by the
/// host (<c>StateSchemaBootstrapper</c>); the client never performs DDL. A regular (logged) table so a
/// suspension survives <c>pg_upgrade</c>; an absent row or table reads as <see cref="StateRunning"/>.
/// </summary>
internal static class ControlContract
{
    public const string Table = "wallaby.control";

    /// <summary>LISTEN/NOTIFY channel signalled on every control-state transition.</summary>
    public const string NotifyChannel = "wallaby_control";

    /// <summary>The single control row's key: suspension is installation-wide, not per-slot.</summary>
    public const string Scope = "wallaby";

    public const string StateRunning = "Running";
    public const string StateSuspendRequested = "SuspendRequested";
    public const string StateSuspended = "Suspended";

    /// <summary>A suspension requested at runtime; persists until an explicit resume.</summary>
    public const string OriginClient = "client";

    /// <summary>
    /// A suspension asserted by a deployed <c>Suspend()</c> builder flag; auto-resumed by a node
    /// deployed without the flag.
    /// </summary>
    public const string OriginConfiguration = "configuration";
}
