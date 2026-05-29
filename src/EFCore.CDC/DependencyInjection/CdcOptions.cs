namespace EFCore.CDC.DependencyInjection;

/// <summary>What to do when a sink permanently fails (or exhausts retries) for a batch.</summary>
public enum CdcDeadLetterPolicy
{
    /// <summary>Stop the pipeline (default); the batch is retried after the leader session restarts.</summary>
    Halt,

    /// <summary>Log and drop the failed batch, then continue (acknowledging the transaction).</summary>
    Skip,
}

/// <summary>Configuration for a CDC instance, set via the fluent builder / <c>ConfigureOptions</c>.</summary>
public sealed class CdcOptions
{
    /// <summary>Connection string to the source Postgres database (used for SQL, state, and replication).</summary>
    public string ConnectionString { get; set; } = "";

    /// <summary>Logical replication slot name.</summary>
    public string SlotName { get; set; } = "efcore_cdc_slot";

    /// <summary>Publication name.</summary>
    public string PublicationName { get; set; } = "efcore_cdc_pub";

    /// <summary>Backfill keyset page size.</summary>
    public int ChunkSize { get; set; } = 500;

    /// <summary>Reconcile an existing publication's table set to match the captured model.</summary>
    public bool ManagePublicationTables { get; set; } = true;

    /// <summary>Fail (instead of warn) when a table needs <c>REPLICA IDENTITY FULL</c> but lacks it.</summary>
    public bool RequireFullReplicaIdentity { get; set; }

    /// <summary>Automatically backfill a newly declared table.</summary>
    public bool AutoBackfillNewTables { get; set; } = true;

    /// <summary>Automatically re-backfill a table when its declared transform version changes.</summary>
    public bool AutoBackfillOnVersionChange { get; set; } = true;

    /// <summary>How long a standby node waits before retrying to acquire leadership.</summary>
    public TimeSpan StandbyRetryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long to wait before retrying after a failed leader session.</summary>
    public TimeSpan LeaderRetryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>What to do when a sink permanently fails (or exhausts retries) for a batch.</summary>
    public CdcDeadLetterPolicy DeadLetterPolicy { get; set; } = CdcDeadLetterPolicy.Halt;
}
