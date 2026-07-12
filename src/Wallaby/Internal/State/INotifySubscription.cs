namespace Wallaby.Internal.State;

/// <summary>
/// A wait handle a worker blocks on between passes. <see cref="WaitAsync"/> returns as soon as the
/// channel is signalled (event-driven wake) or after the fallback timeout elapses (safety poll),
/// whichever is first. Dispose to release any resources (e.g. a dedicated listening connection).
/// </summary>
internal interface INotifySubscription : IAsyncDisposable
{
    /// <summary>Wait until the channel is signalled or <paramref name="fallbackTimeout"/> elapses.</summary>
    Task WaitAsync(TimeSpan fallbackTimeout, CancellationToken ct);
}
