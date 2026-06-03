using Wallaby.Abstractions;
using Wallaby.Diagnostics;

namespace EFCore.CDC.UnitTests;

public class CdcStatusTests
{
    [Test]
    public async Task Starts_in_starting_role_with_unknown_lag()
    {
        var status = new CdcStatus();

        await Assert.That(status.Current.Role).IsEqualTo(CdcNodeRole.Starting);
        await Assert.That(status.Current.LastIngestionLagSeconds).IsEqualTo(-1d);
    }

    [Test]
    public async Task EnterLeader_then_RecordProgress_updates_snapshot()
    {
        var status = new CdcStatus();
        var since = DateTimeOffset.UtcNow;

        status.EnterLeader(since);
        status.RecordProgress(lsn: 42, lagSeconds: 3.5, at: since.AddSeconds(1));

        var snapshot = status.Current;
        await Assert.That(snapshot.Role).IsEqualTo(CdcNodeRole.Leader);
        await Assert.That(snapshot.LeaderSince).IsEqualTo(since);
        await Assert.That(snapshot.LastAcknowledgedLsn).IsEqualTo(42UL);
        await Assert.That(snapshot.LastIngestionLagSeconds).IsEqualTo(3.5);
    }

    [Test]
    public async Task RecordProgress_with_unknown_lag_keeps_the_previous_lag()
    {
        var status = new CdcStatus();

        status.RecordProgress(1, 5.0, DateTimeOffset.UtcNow);
        status.RecordProgress(2, -1, DateTimeOffset.UtcNow);

        await Assert.That(status.Current.LastIngestionLagSeconds).IsEqualTo(5.0);
        await Assert.That(status.Current.LastAcknowledgedLsn).IsEqualTo(2UL);
    }

    [Test]
    public async Task MarkFaulted_sets_faulted_stopped_and_error()
    {
        var status = new CdcStatus();
        status.EnterLeader(DateTimeOffset.UtcNow);

        status.MarkFaulted("Boom: bad");

        await Assert.That(status.Current.Faulted).IsTrue();
        await Assert.That(status.Current.Role).IsEqualTo(CdcNodeRole.Stopped);
        await Assert.That(status.Current.LastError).IsEqualTo("Boom: bad");
    }

    [Test]
    public async Task Leader_failures_increment_and_reset()
    {
        var status = new CdcStatus();

        status.RecordLeaderFailure("e1");
        status.RecordLeaderFailure("e2");
        await Assert.That(status.Current.ConsecutiveLeaderFailures).IsEqualTo(2);

        status.ResetLeaderFailures();
        await Assert.That(status.Current.ConsecutiveLeaderFailures).IsEqualTo(0);
    }

    [Test]
    public async Task EnterStandby_clears_leader_since()
    {
        var status = new CdcStatus();
        status.EnterLeader(DateTimeOffset.UtcNow);

        status.EnterStandby();

        await Assert.That(status.Current.Role).IsEqualTo(CdcNodeRole.Standby);
        await Assert.That(status.Current.LeaderSince).IsNull();
    }
}
