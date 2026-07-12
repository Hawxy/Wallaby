using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Internal;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;
using Wallaby.Model;
using Wallaby.Providers;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Proves spill ownership follows the replication slot: stale <c>wallaby.stream_buffer</c> rows are
/// cleared only once a stream exclusively holds the slot, so a node that loses the slot race (a standby
/// bootstrapping during a takeover while the old leader still streams) can never wipe the live leader's
/// buffer.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class SpillFencingTests(TestModelPostgresFixture pg)
{
    private static WallabyModel BuildTestModel()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        return EfCoreCaptureModelBuilder.Build(
            ctx.Model, new CaptureSpec { DeclaredEntities = [typeof(Category), typeof(Product)] });
    }

    private async Task<WallabyNames> SelfConfigureAsync()
    {
        var names = WallabyNames.Unique();
        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions { SlotName = names.Slot, PublicationName = names.Publication },
            NullLogger.Instance);
        await configurator.EnsureConfiguredAsync(BuildTestModel(), CancellationToken.None);

        await using var conn = await pg.DataSource.OpenConnectionAsync();
        await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
        return names;
    }

    private async Task SeedBufferRowAsync(string slot)
    {
        await using var conn = await pg.DataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO wallaby.stream_buffer (slot_name, xid, subxid, seq, payload) VALUES (@s, 999, 999, 0, @p)", conn);
        cmd.Parameters.AddWithValue("s", slot);
        cmd.Parameters.AddWithValue("p", new byte[] { 1 });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<long> CountBufferRowsAsync(string slot)
    {
        await using var conn = await pg.DataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT count(*) FROM wallaby.stream_buffer WHERE slot_name = @s", conn);
        cmd.Parameters.AddWithValue("s", slot);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    [Test]
    public async Task Stale_spill_rows_are_cleared_once_streaming_begins()
    {
        var names = await SelfConfigureAsync();
        try
        {
            await SeedBufferRowAsync(names.Slot);

            var db = new TestDatabase(pg.ConnectionString);
            var categoryId = await db.AddCategoryAsync();
            await db.AddProductAsync(categoryId, "fencing-clear");

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await using var spill = new PostgresUnloggedTableSpill(pg.DataSource, names.Slot);
            await using var stream = new LogicalReplicationStream(pg.ConnectionString, names.Slot, names.Publication, spill);
            await foreach (var _ in stream.ReadAsync(cts.Token))
            {
                break; // the first message already triggered the clear
            }

            (await CountBufferRowsAsync(names.Slot)).ShouldBe(0);
        }
        finally
        {
            await DropSlotAndPublicationAsync(names);
        }
    }

    [Test]
    public async Task A_node_that_cannot_take_the_slot_does_not_touch_the_spill()
    {
        var names = await SelfConfigureAsync();
        try
        {
            await SeedBufferRowAsync(names.Slot);

            // Node 1 holds the slot: START_REPLICATION runs on first enumeration; with no traffic it
            // receives no message and therefore never clears the seeded rows.
            using var cts = new CancellationTokenSource();
            await using var spill1 = new PostgresUnloggedTableSpill(pg.DataSource, names.Slot);
            await using var stream1 = new LogicalReplicationStream(pg.ConnectionString, names.Slot, names.Publication, spill1);
            var reading = Task.Run(async () =>
            {
                try
                {
                    await foreach (var _ in stream1.ReadAsync(cts.Token)) { }
                }
                catch (OperationCanceledException) { }
            });

            try
            {
                await Polling.UntilAsync(() => SlotActiveAsync(names.Slot), TimeSpan.FromMinutes(2));

                // Node 2 loses the slot race: it must fault without having touched the buffer.
                await using var spill2 = new PostgresUnloggedTableSpill(pg.DataSource, names.Slot);
                await using var stream2 = new LogicalReplicationStream(pg.ConnectionString, names.Slot, names.Publication, spill2);
                var ex = await Should.ThrowAsync<PostgresException>(async () =>
                {
                    await foreach (var _ in stream2.ReadAsync(CancellationToken.None))
                    {
                        break;
                    }
                });
                ex.SqlState.ShouldBe(PostgresErrorCodes.ObjectInUse);

                (await CountBufferRowsAsync(names.Slot)).ShouldBe(1); // the loser never cleared
            }
            finally
            {
                await cts.CancelAsync();
                await reading;
            }
        }
        finally
        {
            await DropSlotAndPublicationAsync(names);
        }
    }

    private async Task<bool> SlotActiveAsync(string slot)
    {
        await using var conn = await pg.DataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT active FROM pg_replication_slots WHERE slot_name = @s", conn);
        cmd.Parameters.AddWithValue("s", slot);
        return await cmd.ExecuteScalarAsync() is true;
    }

    // Slots and publications survive the shared session database, so tests drop their own to avoid
    // exhausting max_replication_slots. The prior stream's replication connection can linger briefly
    // after disposal; retry until the server considers the slot inactive.
    private async Task DropSlotAndPublicationAsync(WallabyNames names)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await PgExec.ExecuteAsync(
                    conn,
                    "SELECT pg_drop_replication_slot(@s) WHERE EXISTS " +
                    "(SELECT 1 FROM pg_replication_slots WHERE slot_name = @s)",
                    default,
                    ("s", names.Slot));
                break;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ObjectInUse && attempt < 50)
            {
                await Task.Delay(100);
            }
        }
        await PgExec.ExecuteAsync(conn, $"DROP PUBLICATION IF EXISTS {PgExec.QuoteIdentifier(names.Publication)}", default);
    }
}
