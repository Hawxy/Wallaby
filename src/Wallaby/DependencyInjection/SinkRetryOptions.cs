namespace Wallaby.DependencyInjection;

/// <summary>
/// Retry policy for sink delivery. A retryable failure is retried with exponential backoff (with jitter)
/// up to <see cref="MaxAttempts"/> times; exhaustion (or a permanent failure) halts the leader session,
/// which then retries with leader-level backoff.
/// </summary>
public sealed class SinkRetryOptions
{
    /// <summary>
    /// Retry attempts after the first delivery try (0–100). <c>0</c> disables in-dispatch retry entirely:
    /// the first retryable failure halts the leader session and leader-level backoff takes over.
    /// </summary>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>Delay before the first retry; later delays grow exponentially. Must be greater than zero.</summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Ceiling on the delay between attempts. Must be at least <see cref="BaseDelay"/>.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(3);
}
