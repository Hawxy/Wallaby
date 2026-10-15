using Wallaby.Client;

namespace Wallaby.Aspire.Hosting.Internal;

/// <summary>Shared between the poll loop, the health check and the commands of one <c>WallabyResource</c>.</summary>
internal sealed class WallabyMonitorState
{
    private readonly SemaphoreSlim _pollNow = new(0, 1);
    private volatile WallabyObservation? _latest;
    private volatile WallabyControlClient? _client;

    public WallabyObservation? Latest
    {
        get => _latest;
        set => _latest = value;
    }

    /// <summary>The client the commands use; null until the database connection string resolves.</summary>
    public WallabyControlClient? Client
    {
        get => _client;
        set => _client = value;
    }

    /// <summary>Wake the poll loop early, e.g. after a command changed state.</summary>
    public void PollNow()
    {
        try
        {
            _pollNow.Release();
        }
        catch (SemaphoreFullException)
        {
            // A wake-up is already pending.
        }
    }

    public Task WaitForNextPollAsync(TimeSpan interval, CancellationToken ct)
        => _pollNow.WaitAsync(interval, ct);
}
