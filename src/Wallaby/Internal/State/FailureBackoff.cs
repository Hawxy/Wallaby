namespace Wallaby.Internal.State;

/// <summary>
/// The failure backoff schedule shared by the persisted work stores (<c>wallaby.backfill_state</c> and
/// <c>wallaby.fanout_queue</c>): delay = <c>min(BaseDelay * 2^min(attempts, 16), MaxDelay)</c>, computed
/// in SQL from the persisted attempt count so it survives restarts and leader changes.
/// </summary>
internal static class FailureBackoff
{
    public static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(5);
}
