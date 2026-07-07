using Wallaby.Internal.Cluster;

namespace Wallaby.Tests.Unit;

public class LeadershipMonitorTests
{
    [Test]
    public async Task Invokes_onLost_and_stops_when_the_probe_reports_lost()
    {
        var checks = 0;
        Func<CancellationToken, Task<bool>> probe = _ => Task.FromResult(++checks <= 1); // held once, then lost
        var lost = false;

        await LeadershipMonitor.WatchAsync(
            probe, TimeSpan.FromMilliseconds(10), () => { lost = true; return Task.CompletedTask; }, CancellationToken.None);

        lost.ShouldBeTrue();
    }

    [Test]
    public async Task Stops_on_cancellation_without_reporting_loss()
    {
        Func<CancellationToken, Task<bool>> probe = _ => Task.FromResult(true); // always held
        var lost = false;
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(60));

        await LeadershipMonitor.WatchAsync(
            probe, TimeSpan.FromMilliseconds(10), () => { lost = true; return Task.CompletedTask; }, cts.Token);

        lost.ShouldBeFalse();
    }
}
