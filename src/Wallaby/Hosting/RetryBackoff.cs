namespace Wallaby.Hosting;

/// <summary>
/// Computes an exponentially-growing, jittered retry delay for the leader loop — so a persistently
/// failing leader session (e.g. a self-config error) backs off instead of hot-looping at a fixed interval.
/// The first delay is ~<c>baseDelay</c>; each subsequent call doubles up to a cap, with ±20% jitter.
/// <see cref="Reset"/> returns to the base after a healthy session.
/// </summary>
internal sealed class RetryBackoff(TimeSpan baseDelay)
{
    private readonly TimeSpan _maxDelay = baseDelay > TimeSpan.FromMinutes(2) ? baseDelay : TimeSpan.FromMinutes(2);
    private int _attempt;

    /// <summary>Reset back to the base delay (call after a healthy run).</summary>
    public void Reset() => _attempt = 0;

    /// <summary>The next delay (and advance the schedule): <c>min(base · 2^n, cap)</c> with ±20% jitter.</summary>
    public TimeSpan Next()
    {
        // Clamp the exponent so base · 2^n can't overflow; 2^16 already dwarfs any sane cap.
        var scaled = baseDelay.TotalMilliseconds * Math.Pow(2, Math.Min(_attempt, 16));
        var capped = Math.Min(scaled, _maxDelay.TotalMilliseconds);
        _attempt++;
        var jitter = 0.8 + (Random.Shared.NextDouble() * 0.4); // ±20%
        return TimeSpan.FromMilliseconds(capped * jitter);
    }
}
