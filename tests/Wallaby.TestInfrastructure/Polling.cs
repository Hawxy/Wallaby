namespace EFCore.CDC.TestInfrastructure;

/// <summary>Polls an asynchronous condition until it holds or a timeout elapses.</summary>
public static class Polling
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(60);
    public static readonly TimeSpan DefaultInterval = TimeSpan.FromMilliseconds(250);

    public static async Task UntilAsync(Func<Task<bool>> predicate, TimeSpan? timeout = null, Action? onTick = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        while (DateTime.UtcNow < deadline)
        {
            onTick?.Invoke();
            if (await predicate())
            {
                return;
            }
            await Task.Delay(DefaultInterval);
        }

        onTick?.Invoke();
        if (await predicate())
        {
            return;
        }

        throw new TimeoutException("Condition was not satisfied within the timeout.");
    }

    public static Task UntilAsync(Func<bool> predicate, TimeSpan? timeout = null, Action? onTick = null)
        => UntilAsync(() => Task.FromResult(predicate()), timeout, onTick);
}
