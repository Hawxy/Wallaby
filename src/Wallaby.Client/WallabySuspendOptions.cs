namespace Wallaby.Client;

/// <summary>Options for <see cref="WallabyControlClient.SuspendAsync"/>.</summary>
public sealed record WallabySuspendOptions
{
    /// <summary>Free-text reason recorded with the suspension (e.g. "PG17 major-version upgrade").</summary>
    public string? Reason { get; init; }

    /// <summary>Who is requesting the suspension. Defaults to the machine name.</summary>
    public string? RequestedBy { get; init; }

    /// <summary>
    /// Wait until the suspension finalizes (every managed slot verified dropped) before returning.
    /// When false, the request is persisted and signalled and the call returns immediately — poll
    /// <see cref="WallabyControlClient.GetStateAsync"/> for completion.
    /// </summary>
    public bool WaitForCompletion { get; init; } = true;

    /// <summary>
    /// How long to leave the slot drops to a running Wallaby host before this client drops them itself.
    /// The fallback covers deployments with no live host (scaled to zero, or provision-only workers that
    /// already exited) and is safe against a live one: an actively streamed slot refuses the drop, so the
    /// client keeps waiting for the host instead.
    /// </summary>
    public TimeSpan HostGracePeriod { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Overall deadline for <see cref="WaitForCompletion"/>. On expiry a
    /// <see cref="WallabyControlTimeoutException"/> carries the last observed state — the request stays
    /// persisted, so the suspension still completes once whatever is holding a slot open lets go.
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(2);

    /// <summary>Receives a state snapshot on every completion poll (roughly once per second).</summary>
    public IProgress<WallabyControlState>? Progress { get; init; }
}
