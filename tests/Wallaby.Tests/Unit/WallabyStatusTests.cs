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
    public void Leader_failures_increment_and_a_clean_session_end_resets_every_streak()
    {
        var status = new WallabyStatus();

        status.RecordLeaderFailure("e1");
        status.RecordLeaderFailure("e2");
        status.RecordFanoutPassFailure("e3");
        status.RecordBackfillTableFailure("e4", attempts: 2);
        status.Current.ConsecutiveLeaderFailures.ShouldBe(2);

        status.ResetFailureStreaks();

        var snapshot = status.Current;
        snapshot.ConsecutiveLeaderFailures.ShouldBe(0);
        snapshot.ConsecutiveFanoutPassFailures.ShouldBe(0);
        snapshot.ConsecutiveBackfillFailures.ShouldBe(0);
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
    public void Fanout_pass_failures_and_the_job_streak_are_separate_facts()
    {
        var status = new WallabyStatus();

        // Pass failures (queue unreachable) never inflate the persisted job streak.
        status.RecordFanoutPassFailure("e1");
        status.RecordFanoutPassFailure("e2");
        status.Current.ConsecutiveFanoutPassFailures.ShouldBe(2);
        status.Current.ConsecutiveFanoutFailures.ShouldBe(0);
        status.Current.LastError.ShouldBe("e2");

        // A job failure mirrors its persisted attempts without touching the pass counter.
        status.RecordFanoutJobFailure("e3", attempts: 4);
        status.Current.ConsecutiveFanoutFailures.ShouldBe(4);
        status.Current.ConsecutiveFanoutPassFailures.ShouldBe(2);

        // A clean pass reconciles the streak from the store and clears the pass counter.
        status.SetFanoutStreak(1);
        status.Current.ConsecutiveFanoutFailures.ShouldBe(1);
        status.Current.ConsecutiveFanoutPassFailures.ShouldBe(0);
    }

    [Test]
    public void Backfill_failures_track_the_worst_table_and_reconcile_from_the_store()
    {
        var status = new WallabyStatus();

        status.RecordBackfillTableFailure("e1", attempts: 3);
        status.RecordBackfillTableFailure("e2", attempts: 1); // another table failing less doesn't lower it
        status.Current.ConsecutiveBackfillFailures.ShouldBe(3);
        status.Current.LastError.ShouldBe("e2");

        status.SetBackfillStreak(0); // the failing table recovered; the reconcile lowers it
        status.Current.ConsecutiveBackfillFailures.ShouldBe(0);
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
    public void EnterSuspended_sets_role_and_context()
    {
        var status = new WallabyStatus();
        var since = DateTimeOffset.UtcNow;
        status.EnterLeader(DateTimeOffset.UtcNow);

        status.EnterSuspended(since, "PG18 upgrade");

        var snapshot = status.Current;
        snapshot.Role.ShouldBe(WallabyNodeRole.Suspended);
        snapshot.LeaderSince.ShouldBeNull();
        snapshot.SuspendedSince.ShouldBe(since);
        snapshot.SuspensionReason.ShouldBe("PG18 upgrade");
    }

    [Test]
    public void Resuming_into_leader_or_standby_clears_suspension_context()
    {
        var status = new WallabyStatus();
        status.EnterSuspended(DateTimeOffset.UtcNow, "upgrade");

        status.EnterStandby();
        status.Current.SuspendedSince.ShouldBeNull();
        status.Current.SuspensionReason.ShouldBeNull();

        status.EnterSuspended(DateTimeOffset.UtcNow, "upgrade");
        status.EnterLeader(DateTimeOffset.UtcNow);
        status.Current.SuspendedSince.ShouldBeNull();
        status.Current.SuspensionReason.ShouldBeNull();
    }

    [Test]
    public void MarkStopped_clears_the_previous_roles_context()
    {
        var status = new WallabyStatus();
        status.EnterLeader(DateTimeOffset.UtcNow);

        status.MarkStopped();

        var snapshot = status.Current;
        snapshot.Role.ShouldBe(WallabyNodeRole.Stopped);
        snapshot.LeaderSince.ShouldBeNull();
        snapshot.Faulted.ShouldBeFalse();

        status.EnterSuspended(DateTimeOffset.UtcNow, "upgrade");
        status.MarkStopped();
        status.Current.SuspendedSince.ShouldBeNull();
        status.Current.SuspensionReason.ShouldBeNull();
    }

    [Test]
    public void Entering_a_role_clears_a_previous_fault()
    {
        var status = new WallabyStatus();
        status.MarkFaulted("Boom: bad");

        status.EnterStandby();

        status.Current.Faulted.ShouldBeFalse();
        status.Current.Role.ShouldBe(WallabyNodeRole.Standby);
    }

    [Test]
    public void EnterSuspended_clears_the_failure_streaks()
    {
        // A crash-looped node entering a planned suspension window must read Suspended (Degraded),
        // not crash-looping (Unhealthy): the health check's crash-loop grade outranks Suspended.
        var status = new WallabyStatus();
        status.RecordLeaderFailure("e1");
        status.RecordLeaderFailure("e2");
        status.RecordBackfillTableFailure("e3", attempts: 7);

        status.EnterSuspended(DateTimeOffset.UtcNow, "upgrade");

        var snapshot = status.Current;
        snapshot.ConsecutiveLeaderFailures.ShouldBe(0);
        snapshot.ConsecutiveBackfillFailures.ShouldBe(0);
    }

    [Test]
    public void EnterStandby_clears_the_failure_streaks()
    {
        var status = new WallabyStatus();
        status.RecordLeaderFailure("e1");
        status.RecordFanoutJobFailure("e2", attempts: 3);

        status.EnterStandby();

        var snapshot = status.Current;
        snapshot.ConsecutiveLeaderFailures.ShouldBe(0);
        snapshot.ConsecutiveFanoutFailures.ShouldBe(0);
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
