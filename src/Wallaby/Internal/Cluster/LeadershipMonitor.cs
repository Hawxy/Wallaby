namespace Wallaby.Internal.Cluster;

/// <summary>
/// A generic "poll until lost" loop: periodically invokes a liveness probe and, as soon as it reports
/// not-held, runs <c>onLost</c> and returns. Used by a cluster-lock handle to heartbeat its underlying
/// resource so a silent loss surfaces promptly. Returns when the probe reports lost or <c>ct</c> cancels.
/// </summary>
internal static class LeadershipMonitor
{
    public static async Task WatchAsync(
        Func<CancellationToken, Task<bool>> isHeld, TimeSpan interval, Func<Task> onLost, CancellationToken ct)
    {
        try
        {
            using var timer = new PeriodicTimer(interval);
            while (await timer.WaitForNextTickAsync(ct))
            {
                if (!await isHeld(ct))
                {
                    await onLost();
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Stopped: shutdown, or the handle is being disposed.
        }
    }
}
