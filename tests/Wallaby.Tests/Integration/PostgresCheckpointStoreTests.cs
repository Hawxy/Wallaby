using Wallaby.Abstractions;
using Wallaby.Internal.State;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

/// <summary>
/// The checkpoint lives on the slot's <c>wallaby.slot_registry</c> row: a registered slot round-trips
/// its checkpoint, an unregistered slot reads as never checkpointed, and the save never invents a
/// registry row (the provisioner owns registration).
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class PostgresCheckpointStoreTests(PostgresFixture pg)
{
    [Test]
    public async Task A_registered_slot_round_trips_its_checkpoint()
    {
        await EnsureSchemaAsync();
        var slot = $"cp_slot_{Guid.NewGuid():N}";
        await RegisterSlotAsync(slot);
        var store = new PostgresCheckpointStore(pg.DataSource);

        // Registered but never checkpointed: reads as no checkpoint, not as LSN 0.
        (await store.GetAsync(slot, CancellationToken.None)).ShouldBeNull();

        await store.SaveAsync(slot, new Checkpoint(0x1_0000002A, DateTimeOffset.UtcNow), CancellationToken.None);

        var read = await store.GetAsync(slot, CancellationToken.None);
        read.ShouldNotBeNull();
        read.ConfirmedLsn.ShouldBe(0x1_0000002AUL);

        await store.SaveAsync(slot, new Checkpoint(0x2_00000001, DateTimeOffset.UtcNow), CancellationToken.None);
        (await store.GetAsync(slot, CancellationToken.None))!.ConfirmedLsn.ShouldBe(0x2_00000001UL);
    }

    [Test]
    public async Task A_save_for_an_unregistered_slot_does_not_invent_a_registry_row()
    {
        await EnsureSchemaAsync();
        var slot = $"cp_ghost_{Guid.NewGuid():N}";
        var store = new PostgresCheckpointStore(pg.DataSource);

        await store.SaveAsync(slot, new Checkpoint(42, DateTimeOffset.UtcNow), CancellationToken.None);

        (await store.GetAsync(slot, CancellationToken.None)).ShouldBeNull();
        await using var cmd = pg.DataSource.CreateCommand(
            "SELECT count(*) FROM wallaby.slot_registry WHERE slot_name = $1");
        cmd.Parameters.AddWithValue(slot);
        Convert.ToInt64(await cmd.ExecuteScalarAsync()).ShouldBe(0L);
    }

    private async Task RegisterSlotAsync(string slot)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            "INSERT INTO wallaby.slot_registry (slot_name, publication) VALUES ($1, $2)");
        cmd.Parameters.AddWithValue(slot);
        cmd.Parameters.AddWithValue($"{slot}_pub");
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task EnsureSchemaAsync()
    {
        await using var conn = await pg.DataSource.OpenConnectionAsync();
        await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
    }
}
