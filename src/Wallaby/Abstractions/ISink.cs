namespace Wallaby.Abstractions;

/// <summary>Outcome category for a sink delivery attempt.</summary>
public enum DeliveryStatus
{
    /// <summary>The batch was accepted by the destination.</summary>
    Success,

    /// <summary>A transient failure; the dispatcher should retry with backoff.</summary>
    RetryableFailure,

    /// <summary>A non-retryable failure; the dispatcher halts the pipeline.</summary>
    PermanentFailure,
}

/// <summary>The result of an <see cref="ISink.DeliverAsync"/> attempt.</summary>
/// <param name="Status">Outcome category.</param>
/// <param name="Error">Optional human-readable error.</param>
/// <param name="Exception">Optional underlying exception.</param>
public sealed record DeliveryResult(DeliveryStatus Status, string? Error = null, Exception? Exception = null)
{
    /// <summary>A shared success result.</summary>
    public static readonly DeliveryResult Success = new(DeliveryStatus.Success);

    /// <summary>Create a retryable failure.</summary>
    public static DeliveryResult Retry(string error, Exception? exception = null)
        => new(DeliveryStatus.RetryableFailure, error, exception);

    /// <summary>Create a permanent failure.</summary>
    public static DeliveryResult Permanent(string error, Exception? exception = null)
        => new(DeliveryStatus.PermanentFailure, error, exception);
}

/// <summary>
/// One transformed record destined for a sink: an upsert of <see cref="Document"/> under
/// <see cref="DocumentId"/>, or a deletion when <see cref="IsDeletion"/> is true.
/// </summary>
/// <param name="Destination">
/// The sink-specific destination (index/topic/table). Null means the sink's default destination.
/// </param>
/// <param name="DocumentId">Stable id for upsert/delete, derived from the source primary key.</param>
/// <param name="Document">The field-bag document to upsert; null when <paramref name="IsDeletion"/> is true.</param>
/// <param name="IsDeletion">True to delete <paramref name="DocumentId"/> from the destination.</param>
/// <param name="Metadata">Source provenance for observability/idempotency.</param>
public sealed record SinkRecord(
    string? Destination,
    string DocumentId,
    IReadOnlyDictionary<string, object?>? Document,
    bool IsDeletion,
    ChangeMetadata Metadata);

/// <summary>A batch of records routed to a single named sink, in commit order.</summary>
/// <param name="SinkName">The target sink's <see cref="ISink.Name"/>.</param>
/// <param name="Records">The records to deliver, in order.</param>
public sealed record SinkBatch(string SinkName, IReadOnlyList<SinkRecord> Records);

/// <summary>
/// A destination plugin. Implementations deliver batches of <see cref="SinkRecord"/>s and should
/// be idempotent (upsert/delete by <see cref="SinkRecord.DocumentId"/>) to honor at-least-once delivery.
/// </summary>
/// <remarks>
/// Registering a sink hands its lifetime to Wallaby: a sink that also implements
/// <see cref="IAsyncDisposable"/> or <see cref="IDisposable"/> is disposed once at host shutdown,
/// after streaming has stopped. Implement <see cref="ISinkInitializer"/> for one-time setup and
/// <see cref="ISinkPurger"/> to support purge-then-backfill convergence.
/// </remarks>
public interface ISink
{
    /// <summary>The unique registration name of this sink (matches mappings' target name).</summary>
    string Name { get; }

    /// <summary>Deliver a batch to the destination, classifying the outcome.</summary>
    Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct);
}

/// <summary>
/// Optional capability: a sink that needs one-time setup before delivery (e.g. creating a
/// Meilisearch index or configuring its primary key).
/// </summary>
public interface ISinkInitializer
{
    /// <summary>Perform idempotent one-time setup for the sink.</summary>
    Task InitializeAsync(CancellationToken ct);
}

/// <summary>
/// Identifies what a purge removes: everything the sink holds at one mapping's destination.
/// </summary>
/// <param name="TableSchema">Schema of the source table about to be backfilled.</param>
/// <param name="TableName">Name of the source table about to be backfilled.</param>
/// <param name="Destination">
/// The sink-specific destination (index/topic/table) to empty. Null means the sink's default
/// destination.
/// </param>
public sealed record SinkPurgeRequest(string TableSchema, string TableName, string? Destination)
{
    /// <summary>Schema-qualified source table name (e.g. <c>public.orders</c>).</summary>
    public string QualifiedTableName => $"{TableSchema}.{TableName}";
}

/// <summary>
/// Optional capability: a sink whose destinations can be emptied so that a fresh backfill converges
/// the destination to exactly the current table contents (removing documents whose source rows
/// disappeared without a delivered delete). Invoked before the backfill's snapshot read when a
/// purge was requested.
/// </summary>
public interface ISinkPurger
{
    /// <summary>
    /// Delete every document at the requested destination. Must be idempotent; throw to fail the
    /// backfill run (the leader retries with backoff and the purge re-runs).
    /// </summary>
    Task PurgeAsync(SinkPurgeRequest request, CancellationToken ct);
}
