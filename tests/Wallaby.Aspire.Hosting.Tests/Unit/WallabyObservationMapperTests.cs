using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Wallaby.Aspire.Hosting.Internal;
using Wallaby.Client;

namespace Wallaby.Aspire.Hosting.Tests.Unit;

public class WallabyObservationMapperTests
{
    private static readonly WallabyManagedSlot Streaming = new("app_slot", "app_pub", WallabyManagedSlotKind.Primary,
        ExistsOnServer: true, Active: true, RetainedWalBytes: 2048);

    private static WallabyObservation Observe(
        IReadOnlyList<WallabyManagedSlot>? slots = null,
        IReadOnlyList<WallabyBackfillState>? backfills = null,
        WallabySuspensionState state = WallabySuspensionState.Running,
        bool widened = false,
        string? error = null,
        bool previouslyCaughtUp = true)
        => new(
            new WallabyControlState(state, WallabySuspensionOrigin.Client, null, null, null, null, null,
                slots ?? [Streaming], PublicationsWidened: widened),
            backfills ?? [new WallabyBackfillState("public.orders", WallabyBackfillStatus.Completed, 10, DateTimeOffset.UtcNow)],
            error,
            previouslyCaughtUp);

    [Test]
    public void Streaming_with_every_backfill_complete_is_healthy()
        => WallabyObservationMapper.Health(Observe()).Status.ShouldBe(HealthStatus.Healthy);

    [Test]
    public void A_first_caught_up_poll_waits_for_a_second_before_reporting_healthy()
    {
        var first = WallabyObservationMapper.Health(Observe(previouslyCaughtUp: false));
        first.Status.ShouldBe(HealthStatus.Degraded);
        first.Description.ShouldBe("Caught up; confirming on the next poll.");
    }

    [Test]
    public void Widened_publications_stay_healthy_but_say_so()
    {
        var health = WallabyObservationMapper.Health(Observe(widened: true));
        health.Status.ShouldBe(HealthStatus.Healthy);
        health.Description.ShouldNotBeNull().ShouldContain("widened");
    }

    [Test]
    public void A_pending_backfill_is_degraded_until_it_completes()
    {
        var running = new WallabyBackfillState("public.orders", WallabyBackfillStatus.InProgress, 250, DateTimeOffset.UtcNow)
        {
            EstimatedRows = 1000,
            StartedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
        };

        var health = WallabyObservationMapper.Health(Observe(backfills: [running]));

        health.Status.ShouldBe(HealthStatus.Degraded);
        health.Description.ShouldNotBeNull().ShouldContain("public.orders (in progress, 25%");
    }

    [Test]
    public void A_failing_backfill_reports_its_error()
    {
        var failing = new WallabyBackfillState("public.orders", WallabyBackfillStatus.Requested, 0, DateTimeOffset.UtcNow)
        {
            Attempts = 2,
            LastError = "sink unreachable",
            NextAttemptAt = DateTimeOffset.UtcNow.AddSeconds(20),
        };

        var health = WallabyObservationMapper.Health(Observe(backfills: [failing]));

        health.Status.ShouldBe(HealthStatus.Degraded);
        health.Description.ShouldNotBeNull().ShouldContain("attempt 2 failed: sink unreachable");
    }

    [Test]
    public void No_slots_means_waiting_for_a_host()
    {
        var health = WallabyObservationMapper.Health(Observe(slots: [], backfills: []));
        health.Status.ShouldBe(HealthStatus.Degraded);
        health.Description.ShouldNotBeNull().ShouldContain("Waiting for a Wallaby host");
    }

    [Test]
    public void A_primary_slot_without_a_consumer_is_degraded_but_an_idle_external_one_is_not()
    {
        WallabyObservationMapper.Health(Observe(slots: [Streaming with { Active = false }])).Status
            .ShouldBe(HealthStatus.Degraded);

        var external = new WallabyManagedSlot("ext_slot", "ext_pub", WallabyManagedSlotKind.External, ExistsOnServer: true, Active: false);
        WallabyObservationMapper.Health(Observe(slots: [Streaming, external])).Status.ShouldBe(HealthStatus.Healthy);
    }

    [Test]
    public void Suspension_is_degraded()
    {
        WallabyObservationMapper.Health(Observe(slots: [], state: WallabySuspensionState.Suspended)).Status
            .ShouldBe(HealthStatus.Degraded);
        WallabyObservationMapper.Health(Observe(state: WallabySuspensionState.SuspendRequested)).Status
            .ShouldBe(HealthStatus.Degraded);
    }

    [Test]
    public void Invalidated_slots_poll_errors_and_newer_hosts_are_unhealthy()
    {
        WallabyObservationMapper.Health(Observe(slots: [Streaming with { InvalidationReason = "wal_removed" }])).Status
            .ShouldBe(HealthStatus.Unhealthy);
        WallabyObservationMapper.Health(Observe(error: "connection refused")).Status
            .ShouldBe(HealthStatus.Unhealthy);
        WallabyObservationMapper.Health(Observe(state: WallabySuspensionState.Unknown)).Status
            .ShouldBe(HealthStatus.Unhealthy);
        WallabyObservationMapper.Health(Observe(slots: [Streaming with { Kind = WallabyManagedSlotKind.Unknown }])).Status
            .ShouldBe(HealthStatus.Unhealthy);
    }

    [Test]
    public void Properties_describe_suspension_slots_and_backfills()
    {
        var properties = WallabyObservationMapper.Properties(Observe(error: "timeout"))
            .ToDictionary(p => p.DisplayName!, p => p.Value);

        properties["Suspension"].ShouldBe("Running");
        properties["Slot app_slot"].ShouldBe("streaming, 2 KB WAL retained");
        properties["Backfill public.orders"].ShouldBe("completed, 10 rows");
        properties["Poll error"].ShouldBe("timeout");
        properties.ContainsKey("Publications").ShouldBeFalse();
    }

    [Test]
    public void Merging_replaces_the_previous_poll_and_keeps_aspires_own_properties()
    {
        var first = WallabyObservationMapper.MergeProperties(
            [new ResourcePropertySnapshot("resource.parentName", "worker")],
            WallabyObservationMapper.Properties(Observe(error: "timeout")));

        var second = WallabyObservationMapper.MergeProperties(first, WallabyObservationMapper.Properties(Observe()));

        second.Select(p => p.Name).ShouldBe(
            ["resource.parentName", "wallaby.suspension", "wallaby.slot.app_slot", "wallaby.backfill.public.orders"]);
        second.Where(p => p.Name.StartsWith("wallaby.")).ShouldAllBe(p => p.IsHighlighted);
    }

    [Test]
    public void The_health_report_changes_only_when_the_verdict_does()
    {
        var healthy = WallabyObservationMapper.Health(Observe());
        var first = WallabyObservationMapper.MergeHealthReport([], "cdc_wallaby", healthy);
        first.ShouldHaveSingleItem().Status.ShouldBe(HealthStatus.Healthy);

        WallabyObservationMapper.MergeHealthReport(first, "cdc_wallaby", healthy).ShouldBe(first);

        var suspended = WallabyObservationMapper.MergeHealthReport(
            first, "cdc_wallaby", WallabyObservationMapper.Health(Observe(state: WallabySuspensionState.Suspended)));
        suspended.ShouldHaveSingleItem().Status.ShouldBe(HealthStatus.Degraded);
    }

    [Test]
    public async Task Command_availability_follows_the_latest_poll()
    {
        await using var client = new WallabyControlClient("Host=localhost;Database=unused");
        var monitor = new WallabyMonitorState();

        WallabyObservationMapper.CanBackfill(monitor).ShouldBeFalse(); // not connected

        monitor.Client = client;
        monitor.Latest = Observe();
        WallabyObservationMapper.CanBackfill(monitor).ShouldBeTrue();
        WallabyObservationMapper.CanSuspend(monitor).ShouldBeTrue();
        WallabyObservationMapper.CanResume(monitor).ShouldBeFalse();
        WallabyObservationMapper.CanCancelBackfill(monitor).ShouldBeFalse();
        WallabyObservationMapper.CanWiden(monitor).ShouldBeTrue();
        WallabyObservationMapper.CanRestore(monitor).ShouldBeFalse();

        monitor.Latest = Observe(state: WallabySuspensionState.Suspended,
            backfills: [new WallabyBackfillState("public.orders", WallabyBackfillStatus.Requested, 0, DateTimeOffset.UtcNow)]);
        WallabyObservationMapper.CanBackfill(monitor).ShouldBeFalse();
        WallabyObservationMapper.CanResume(monitor).ShouldBeTrue();
        WallabyObservationMapper.CanCancelBackfill(monitor).ShouldBeTrue();

        monitor.Latest = Observe(widened: true);
        WallabyObservationMapper.CanWiden(monitor).ShouldBeFalse();
        WallabyObservationMapper.CanRestore(monitor).ShouldBeTrue();
    }

    [Test]
    [Arguments(512L, "512 B")]
    [Arguments(1536L, "1.5 KB")]
    [Arguments(5L * 1024 * 1024 * 1024, "5 GB")]
    public void Bytes_are_humanized(long bytes, string expected)
        => WallabyObservationMapper.FormatBytes(bytes).ShouldBe(expected);
}
