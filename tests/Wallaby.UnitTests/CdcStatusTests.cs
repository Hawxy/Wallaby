using Wallaby.Abstractions;
using Wallaby.Diagnostics;

namespace Wallaby.UnitTests;

public class CdcStatusTests
{
    [Test]
    public void Starts_in_starting_role_with_unknown_lag()
    {
        var status = new CdcStatus();

        status.Current.Role.ShouldBe(CdcNodeRole.Starting);
        status.Current.LastIngestionLagSeconds.ShouldBe(-1d);
    }

    [Test]
    public void EnterLeader_then_RecordProgress_updates_snapshot()
    {
        var status = new CdcStatus();
        var since = DateTimeOffset.UtcNow;

        status.EnterLeader(since);
        status.RecordProgress(lsn: 42, lagSeconds: 3.5, at: since.AddSeconds(1));

        var snapshot = status.Current;
        snapshot.Role.ShouldBe(CdcNodeRole.Leader);
        snapshot.LeaderSince.ShouldBe(since);
        snapshot.LastAcknowledgedLsn.ShouldBe(42UL);
        snapshot.LastIngestionLagSeconds.ShouldBe(3.5);
    }

    [Test]
    public void RecordProgress_with_unknown_lag_keeps_the_previous_lag()
    {
        var status = new CdcStatus();

        status.RecordProgress(1, 5.0, DateTimeOffset.UtcNow);
        status.RecordProgress(2, -1, DateTimeOffset.UtcNow);

        status.Current.LastIngestionLagSeconds.ShouldBe(5.0);
        status.Current.LastAcknowledgedLsn.ShouldBe(2UL);
    }

    [Test]
    public void MarkFaulted_sets_faulted_stopped_and_error()
    {
        var status = new CdcStatus();
        status.EnterLeader(DateTimeOffset.UtcNow);

        status.MarkFaulted("Boom: bad");

        status.Current.Faulted.ShouldBeTrue();
        status.Current.Role.ShouldBe(CdcNodeRole.Stopped);
        status.Current.LastError.ShouldBe("Boom: bad");
    }

    [Test]
    public void Leader_failures_increment_and_reset()
    {
        var status = new CdcStatus();

        status.RecordLeaderFailure("e1");
        status.RecordLeaderFailure("e2");
        status.Current.ConsecutiveLeaderFailures.ShouldBe(2);

        status.ResetLeaderFailures();
        status.Current.ConsecutiveLeaderFailures.ShouldBe(0);
    }

    [Test]
    public void EnterStandby_clears_leader_since()
    {
        var status = new CdcStatus();
        status.EnterLeader(DateTimeOffset.UtcNow);

        status.EnterStandby();

        status.Current.Role.ShouldBe(CdcNodeRole.Standby);
        status.Current.LeaderSince.ShouldBeNull();
    }
}
