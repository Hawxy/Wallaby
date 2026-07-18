using Microsoft.Extensions.Diagnostics.HealthChecks;
using Wallaby.Abstractions;
using Wallaby.AspNetCore.HealthChecks;

namespace Wallaby.Tests.Unit;

public class WallabyHealthCheckTests
{
    private sealed class FakeStatus(WallabyStatusSnapshot snapshot) : IWallabyStatus
    {
        public WallabyStatusSnapshot Current => snapshot;
    }

    private static WallabyStatusSnapshot Snap(WallabyNodeRole role, bool faulted = false) => new()
    {
        Role = role,
        Faulted = faulted,
        StartedAt = DateTimeOffset.UtcNow,
        SlotName = "slot",
    };

    private static async Task<HealthStatus> CheckAsync(WallabyStatusSnapshot snapshot)
    {
        var check = new WallabyHealthCheck(new FakeStatus(snapshot));
        var result = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);
        return result.Status;
    }

    [Test]
    [Arguments(WallabyNodeRole.Starting)]
    [Arguments(WallabyNodeRole.Leader)]
    [Arguments(WallabyNodeRole.Standby)]
    public async Task Healthy_while_the_subsystem_is_alive(WallabyNodeRole role)
    {
        (await CheckAsync(Snap(role))).ShouldBe(HealthStatus.Healthy);
    }

    [Test]
    public async Task Unhealthy_when_the_background_service_terminated()
    {
        (await CheckAsync(Snap(WallabyNodeRole.Stopped, faulted: true))).ShouldBe(HealthStatus.Unhealthy);
    }

    [Test]
    public async Task Degraded_while_suspended_with_context_in_data()
    {
        var since = DateTimeOffset.UtcNow;
        var snapshot = Snap(WallabyNodeRole.Suspended) with
        {
            SuspendedSince = since,
            SuspensionReason = "PG18 upgrade",
        };
        var check = new WallabyHealthCheck(new FakeStatus(snapshot));

        var result = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        // Deliberate but loud: the node is alive (don't restart-loop it) while replication is stopped.
        result.Status.ShouldBe(HealthStatus.Degraded);
        result.Data["suspendedSince"].ShouldBe(since);
        result.Data["suspensionReason"].ShouldBe("PG18 upgrade");
    }

    [Test]
    public async Task Fanout_failures_stay_healthy_and_appear_in_data()
    {
        var snapshot = Snap(WallabyNodeRole.Leader) with { ConsecutiveFanoutFailures = 3 };
        var check = new WallabyHealthCheck(new FakeStatus(snapshot));

        var result = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        // A stuck-retrying fan-out is degraded, not dead — a restart wouldn't fix it, so the node stays Healthy.
        result.Status.ShouldBe(HealthStatus.Healthy);
        result.Data["consecutiveFanoutFailures"].ShouldBe(3);
    }
}
