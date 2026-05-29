namespace EFCore.CDC.Abstractions;

/// <summary>Outcome category for a sink delivery attempt.</summary>
public enum DeliveryStatus
{
    /// <summary>The batch was accepted by the destination.</summary>
    Success,

    /// <summary>A transient failure; the dispatcher should retry with backoff.</summary>
    RetryableFailure,

    /// <summary>A non-retryable failure; the dispatcher applies the dead-letter policy.</summary>
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
/// <param name="Document">The document payload to upsert; null when <paramref name="IsDeletion"/> is true.</param>
/// <param name="IsDeletion">True to delete <paramref name="DocumentId"/> from the destination.</param>
/// <param name="Metadata">Source provenance for observability/idempotency.</param>
public sealed record SinkRecord(
    string? Destination,
    string DocumentId,
    object? Document,
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
