using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Replication;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

/// <summary>
/// The slot-lag sampler reads the server's retained-WAL bytes for a slot and caches them for the
/// <c>wallaby.slot.retained_wal</c> gauge.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class SlotLagSamplerTests(PostgresFixture pg)
{
    [Test]
    public async Task Sampler_publishes_retained_wal_for_an_existing_slot()
    {
        var slot = $"lag_slot_{Guid.NewGuid():N}";
        await ExecAsync($"SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')");
        try
        {
            using var instrumentation = new WallabyInstrumentation();
            using var gauge = new MetricCollector<long>(instrumentation.Meter, "wallaby.slot.retained_wal");

            var sampler = new SlotLagSampler(
                pg.DataSource, slot, TimeSpan.FromMilliseconds(100), instrumentation, NullLogger.Instance);
            using var cts = new CancellationTokenSource();
            var run = sampler.RunAsync(cts.Token);

            // Observable gauges are pull-based: poll until the sampler's first cached sample is visible.
            var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
            while (gauge.GetMeasurementSnapshot().Count == 0 && DateTimeOffset.UtcNow < deadline)
            {
                gauge.RecordObservableInstruments();
                await Task.Delay(50);
            }

            await cts.CancelAsync();
            await Should.ThrowAsync<OperationCanceledException>(async () => await run);

            var snapshot = gauge.GetMeasurementSnapshot();
            snapshot.ShouldNotBeEmpty();
            snapshot[^1].Value.ShouldBeGreaterThanOrEqualTo(0L);
            snapshot[^1].Tags["wallaby.slot"].ShouldBe(slot);
        }
        finally
        {
            await ExecAsync(
                $"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{slot}'");
        }
    }

    [Test]
    public async Task A_missing_slot_produces_no_sample()
    {
        using var instrumentation = new WallabyInstrumentation();
        using var gauge = new MetricCollector<long>(instrumentation.Meter, "wallaby.slot.retained_wal");

        var sampler = new SlotLagSampler(
            pg.DataSource, $"absent_{Guid.NewGuid():N}", TimeSpan.FromMilliseconds(50),
            instrumentation, NullLogger.Instance);
        using var cts = new CancellationTokenSource();
        var run = sampler.RunAsync(cts.Token);

        // Give the sampler several ticks; the gauge must stay silent rather than emit a bogus value.
        await Task.Delay(300);
        gauge.RecordObservableInstruments();

        await cts.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(async () => await run);

        gauge.GetMeasurementSnapshot().ShouldBeEmpty();
    }

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }
}
