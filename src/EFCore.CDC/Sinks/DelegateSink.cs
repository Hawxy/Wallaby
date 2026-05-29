using EFCore.CDC.Abstractions;

namespace EFCore.CDC.Sinks;

/// <summary>
/// An in-process sink that forwards each delivered batch to a handler delegate.  The handler should be
/// idempotent (keyed by <see cref="SinkRecord.DocumentId"/>) to honor at-least-once delivery.
/// </summary>
public sealed class DelegateSink(
    string name, Func<SinkBatch, CancellationToken, Task<DeliveryResult>> handler) : ISink
{
    /// <inheritdoc />
    public string Name { get; } = name;

    /// <inheritdoc />
    public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct) => handler(batch, ct);
}
