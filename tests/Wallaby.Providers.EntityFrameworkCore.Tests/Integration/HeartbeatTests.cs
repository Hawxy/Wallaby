using NpgsqlTypes;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Idle-slot heartbeat behaviour. Since Postgres 15 pgoutput skips empty transactions, so a slot whose
/// mapped tables are quiet receives nothing to acknowledge while unrelated tables churn WAL; the
/// heartbeat message keeps <c>confirmed_flush_lsn</c> advancing through the normal ack path.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class HeartbeatTests(TestModelPostgresFixture pg)
{
    [Test]
    public async Task Without_a_heartbeat_unrelated_churn_never_advances_the_slot()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        var noise = await CreateNoiseTableAsync();
        try
        {
            await harness.StartAsync();
            var initial = await ConfirmedFlushLsnAsync(harness.Names.Slot);

            // Only unpublished-table churn: pgoutput skips these transactions entirely, so nothing
            // reaches the pipeline and nothing is acknowledged. This pins the behaviour the heartbeat
            // exists to counter.
            await ChurnAsync(noise, TimeSpan.FromSeconds(2));

            (await ConfirmedFlushLsnAsync(harness.Names.Slot)).ShouldBe(initial);
            harness.LastAcknowledgedLsn.ShouldBe(0UL);
        }
        finally
        {
            await harness.StopAsync();
            await DropNoiseTableAsync(noise);
        }
    }

    [Test]
    public async Task Heartbeat_advances_the_slot_and_checkpoint_while_mapped_tables_are_idle()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        harness.AddCaptureSink();
        harness.HeartbeatInterval = TimeSpan.FromMilliseconds(500);
        await harness.SelfConfigureAsync();

        var noise = await CreateNoiseTableAsync();
        try
        {
            await harness.StartAsync();
            var initial = await ConfirmedFlushLsnAsync(harness.Names.Slot);

            var churn = ChurnAsync(noise, TimeSpan.FromSeconds(8));
            // No mapped-table writes at all — only the heartbeat can advance the slot.
            await harness.WaitUntilAsync(
                async () => await ConfirmedFlushLsnAsync(harness.Names.Slot) > initial,
                TimeSpan.FromSeconds(30));
            await churn;

            // The heartbeat rode the ordinary delivery path, so the checkpoint row advanced with it.
            (await CheckpointLsnAsync(harness.Names.Slot)).ShouldBeGreaterThan(0UL);
            harness.LastAcknowledgedLsn.ShouldBeGreaterThan(0UL);
        }
        finally
        {
            await harness.StopAsync();
            await DropNoiseTableAsync(noise);
        }
    }

    [Test]
    public async Task Heartbeat_is_suppressed_while_real_traffic_is_acknowledged()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var sink = harness.AddCaptureSink();
        harness.HeartbeatInterval = TimeSpan.FromMilliseconds(200);
        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.StartAsync();
        try
        {
            // Mapped writes land far denser than the heartbeat interval, so every tick observes a fresh
            // acknowledged LSN and skips. One emission is tolerated: the first tick can race the first ack.
            var until = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(1.5);
            var produced = 0;
            while (DateTimeOffset.UtcNow < until)
            {
                await harness.Db.AddProductAsync(categoryId, $"hb-{produced++}");
                await Task.Delay(50);
            }

            await harness.WaitUntilAsync(() => sink.Records.Count >= produced);
            harness.HeartbeatsEmitted.ShouldBeLessThanOrEqualTo(1);
        }
        finally
        {
            await harness.StopAsync();
        }
    }

    // ---- helpers ----

    private async Task<string> CreateNoiseTableAsync()
    {
        var table = $"hb_noise_{Guid.NewGuid():N}";
        await ExecAsync($"CREATE TABLE {table} (id int PRIMARY KEY, payload text)");
        return table;
    }

    private Task DropNoiseTableAsync(string table) => ExecAsync($"DROP TABLE IF EXISTS {table}");

    /// <summary>Generate WAL on a table outside the publication for roughly <paramref name="duration"/>.</summary>
    private async Task ChurnAsync(string table, TimeSpan duration)
    {
        var until = DateTimeOffset.UtcNow + duration;
        var i = 0;
        while (DateTimeOffset.UtcNow < until)
        {
            await ExecAsync(
                $"INSERT INTO {table} (id, payload) VALUES ({i++}, 'x') " +
                "ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload");
            await Task.Delay(50);
        }
    }

    private async Task<ulong> ConfirmedFlushLsnAsync(string slot)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1");
        cmd.Parameters.AddWithValue(slot);
        return await cmd.ExecuteScalarAsync() is NpgsqlLogSequenceNumber lsn ? (ulong)lsn : 0UL;
    }

    private async Task<ulong> CheckpointLsnAsync(string slot)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            "SELECT confirmed_lsn FROM wallaby.checkpoint WHERE slot_name = $1");
        cmd.Parameters.AddWithValue(slot);
        return await cmd.ExecuteScalarAsync() is NpgsqlLogSequenceNumber lsn ? (ulong)lsn : 0UL;
    }

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }
}
