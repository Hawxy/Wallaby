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

    /// <summary>
    /// The migration ledger the host's schema bootstrapper maintains; the client reads it to adapt to
    /// (or refuse) older schemas instead of probing for individual columns.
    /// </summary>
    public const string SchemaVersionLedger = "wallaby.schema_version";

    /// <summary>
    /// The wallaby state-schema version this build was compiled against. The host's migration list
    /// aliases it (<c>StateSchemaMigrations.CurrentVersion</c>), and the client requires it for every
    /// state-changing operation except resume — resume stays version-tolerant so an old installation
    /// can always be unsuspended. Version 5 is the oldest schema any deployment carries, so nothing
    /// branches on (or gates against) anything older.
    /// </summary>
    public const int SchemaVersion = 8;

    /// <summary>Schema version that added the <c>control</c> publication-widening columns.</summary>
    public const int WideningSchemaVersion = 6;

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
