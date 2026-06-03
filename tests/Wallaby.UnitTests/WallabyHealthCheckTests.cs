using Microsoft.Extensions.Diagnostics.HealthChecks;
using Wallaby.Abstractions;
using Wallaby.AspNetCore.HealthChecks;

namespace EFCore.CDC.UnitTests;

public class WallabyHealthCheckTests
{
    private sealed class FakeStatus(CdcStatusSnapshot snapshot) : ICdcStatus
    {
        public CdcStatusSnapshot Current => snapshot;
    }

    private static CdcStatusSnapshot Snap(CdcNodeRole role, bool faulted = false) => new()
    {
        Role = role,
        Faulted = faulted,
        StartedAt = DateTimeOffset.UtcNow,
        SlotName = "slot",
    };

    private static async Task<HealthStatus> CheckAsync(CdcStatusSnapshot snapshot)
    {
        var check = new WallabyHealthCheck(new FakeStatus(snapshot));
        var result = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);
        return result.Status;
    }

    [Test]
    [Arguments(CdcNodeRole.Starting)]
    [Arguments(CdcNodeRole.Leader)]
    [Arguments(CdcNodeRole.Standby)]
    public async Task Healthy_while_the_subsystem_is_alive(CdcNodeRole role)
    {
        await Assert.That(await CheckAsync(Snap(role))).IsEqualTo(HealthStatus.Healthy);
    }

    [Test]
    public async Task Unhealthy_when_the_background_service_terminated()
    {
        await Assert.That(await CheckAsync(Snap(CdcNodeRole.Stopped, faulted: true))).IsEqualTo(HealthStatus.Unhealthy);
    }
}
