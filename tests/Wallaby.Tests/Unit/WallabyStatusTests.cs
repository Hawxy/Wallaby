using Wallaby.Abstractions;
using Wallaby.Diagnostics;

namespace Wallaby.Tests.Unit;

public class WallabyStatusTests
{
    [Test]
    public void Starts_in_starting_role_with_unknown_lag()
    {
        var status = new WallabyStatus();

        status.Current.Role.ShouldBe(WallabyNodeRole.Starting);
        status.Current.LastIngestionLagSeconds.ShouldBe(-1d);
    }

    [Test]
    public void EnterLeader_then_RecordProgress_updates_snapshot()
    {
        var status = new WallabyStatus();
        var since = DateTimeOffset.UtcNow;

        status.EnterLeader(since);
        status.RecordProgress(lsn: 42, lagSeconds: 3.5, at: since.AddSeconds(1));

        var snapshot = status.Current;
        snapshot.Role.ShouldBe(WallabyNodeRole.Leader);
        snapshot.LeaderSince.ShouldBe(since);
        snapshot.LastAcknowledgedLsn.ShouldBe(42UL);
        snapshot.LastIngestionLagSeconds.ShouldBe(3.5);
    }

    [Test]
    public void RecordProgress_with_unknown_lag_keeps_the_previous_lag()
    {
        var status = new WallabyStatus();

        status.RecordProgress(1, 5.0, DateTimeOffset.UtcNow);
        status.RecordProgress(2, -1, DateTimeOffset.UtcNow);

        status.Current.LastIngestionLagSeconds.ShouldBe(5.0);
        status.Current.LastAcknowledgedLsn.ShouldBe(2UL);
    }

    [Test]
    public void MarkFaulted_sets_faulted_stopped_and_error()
    {
        var status = new WallabyStatus();
        status.EnterLeader(DateTimeOffset.UtcNow);

        status.MarkFaulted("Boom: bad");

        status.Current.Faulted.ShouldBeTrue();
        status.Current.Role.ShouldBe(WallabyNodeRole.Stopped);
        status.Current.LastError.ShouldBe("Boom: bad");
    }

    [Test]
    public void Leader_failures_increment_and_reset()
    {
        var status = new WallabyStatus();

        status.RecordLeaderFailure("e1");
        status.RecordLeaderFailure("e2");
        status.Current.ConsecutiveLeaderFailures.ShouldBe(2);

        status.ResetLeaderFailures();
        status.Current.ConsecutiveLeaderFailures.ShouldBe(0);
    }

    [Test]
    public void RecordProgress_clears_accumulated_leader_failures()
    {
        var status = new WallabyStatus();

        status.RecordLeaderFailure("e1");
        status.RecordLeaderFailure("e2");
        status.RecordLeaderFailure("e3");
        status.Current.ConsecutiveLeaderFailures.ShouldBe(3);

        status.RecordProgress(lsn: 7, lagSeconds: 0.5, at: DateTimeOffset.UtcNow);

        status.Current.ConsecutiveLeaderFailures.ShouldBe(0);
    }

    [Test]
    public void Fanout_failures_increment_set_the_error_and_reset()
    {
        var status = new WallabyStatus();

        status.RecordFanoutFailure("e1");
        status.RecordFanoutFailure("e2");
        status.Current.ConsecutiveFanoutFailures.ShouldBe(2);
        status.Current.LastError.ShouldBe("e2");

        status.ResetFanoutFailures();
        status.Current.ConsecutiveFanoutFailures.ShouldBe(0);
    }

    [Test]
    public void EnterStandby_clears_leader_since()
    {
        var status = new WallabyStatus();
        status.EnterLeader(DateTimeOffset.UtcNow);

        status.EnterStandby();

        status.Current.Role.ShouldBe(WallabyNodeRole.Standby);
        status.Current.LeaderSince.ShouldBeNull();
    }

    [Test]
    public void Sink_deliveries_surface_in_the_snapshot()
    {
        var status = new WallabyStatus();
        var at = DateTimeOffset.UtcNow;

        status.RecordSinkDelivered("search", at);
        status.RecordSinkDelivered("audit", at.AddSeconds(1));

        var snapshot = status.Current;
        snapshot.LastSinkDeliveryAt["search"].ShouldBe(at);
        snapshot.LastSinkDeliveryAt["audit"].ShouldBe(at.AddSeconds(1));
    }

    [Test]
    public void A_previously_read_snapshot_is_not_mutated_by_later_deliveries()
    {
        var status = new WallabyStatus();
        var at = DateTimeOffset.UtcNow;
        status.RecordSinkDelivered("search", at);

        var before = status.Current;
        status.RecordSinkDelivered("search", at.AddMinutes(1));
        status.RecordSinkDelivered("audit", at.AddMinutes(1));

        before.LastSinkDeliveryAt["search"].ShouldBe(at);
        before.LastSinkDeliveryAt.ShouldNotContainKey("audit");
    }

    [Test]
    public void The_latest_delivery_per_sink_wins()
    {
        var status = new WallabyStatus();
        var at = DateTimeOffset.UtcNow;

        status.RecordSinkDelivered("search", at);
        status.RecordSinkDelivered("search", at.AddSeconds(5));

        status.Current.LastSinkDeliveryAt["search"].ShouldBe(at.AddSeconds(5));
    }
}
