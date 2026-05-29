namespace EFCore.CDC.Internal.SelfConfig;

/// <summary>Inputs to self-configuration. Populated from the consumer's CDC options at startup.</summary>
internal sealed class SelfConfigOptions
{
    /// <summary>The logical replication slot name to create/reuse.</summary>
    public required string SlotName { get; init; }

    /// <summary>The publication name to create/reuse.</summary>
    public required string PublicationName { get; init; }

    /// <summary>
    /// When true, an existing publication's table set is reconciled to match the captured model
    /// (ADD/DROP TABLE). When false, an existing publication is used as-is.
    /// </summary>
    public bool ManagePublicationTables { get; init; } = true;

    /// <summary>
    /// When true, a table that needs <c>REPLICA IDENTITY FULL</c> but doesn't have it causes a hard
    /// failure instead of a warning.
    /// </summary>
    public bool RequireFullReplicaIdentity { get; init; }
}
