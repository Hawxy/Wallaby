namespace Wallaby.Client;

/// <summary>Options for <see cref="WallabyControlClient.WidenPublicationsAsync"/>.</summary>
public sealed record WallabyWidenOptions
{
    /// <summary>Who is requesting the widening. Defaults to the machine name.</summary>
    public string? RequestedBy { get; init; }

    /// <summary>
    /// Wait until every managed publication is verified widened (no column lists or row filters remain)
    /// before returning — i.e. until the blocked migration will actually run. When false, the request is
    /// persisted and signalled and the call returns immediately — poll
    /// <see cref="WallabyControlClient.GetStateAsync"/> for completion.
    /// </summary>
    public bool WaitForCompletion { get; init; } = true;

    /// <summary>
    /// How long to leave the widening to a running Wallaby host (it applies the change by bouncing its
    /// leader session) before this client rewrites the publications itself. The fallback covers
    /// deployments with no live host and is idempotent against a host applying it concurrently.
    /// </summary>
    public TimeSpan HostGracePeriod { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Overall deadline for <see cref="WaitForCompletion"/>. On expiry a
    /// <see cref="WallabyControlTimeoutException"/> carries the last observed state — the request stays
    /// persisted, so the widening still completes once a host applies it.
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(2);

    /// <summary>Receives a state snapshot on every completion poll (roughly once per second).</summary>
    public IProgress<WallabyControlState>? Progress { get; init; }
}
