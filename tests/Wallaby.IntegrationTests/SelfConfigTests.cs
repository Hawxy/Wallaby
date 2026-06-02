using EFCore.CDC.TestInfrastructure;
using EFCore.CDC.TestModel;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;
using Wallaby;
using Wallaby.Internal;
using Wallaby.Internal.SelfConfig;
using Wallaby.Model;

namespace EFCore.CDC.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class SelfConfigTests(PostgresFixture pg)
{
    private static CdcModel BuildAllMappedModel()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        return ModelToCdcModel.Build(ctx.Model, new CaptureSpec { CaptureAllMapped = true });
    }

    private PostgresSelfConfigurator CreateConfigurator(string slot, string pub) =>
        new(pg.DataSource,
            new SelfConfigOptions { SlotName = slot, PublicationName = pub },
            NullLogger.Instance);

    [Test]
    public async Task Creates_publication_slot_and_state_schema()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        var pub = $"cdc_pub_{suffix}";
        var model = BuildAllMappedModel();

        var result = await CreateConfigurator(slot, pub).EnsureConfiguredAsync(model, CancellationToken.None);

        await Assert.That(result.PublicationCreated).IsTrue();
        await Assert.That(result.SlotCreated).IsTrue();
        await Assert.That(result.ConsistentPoint).IsNotNull();

        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();

        await Assert.That(await PgExec.ScalarLongAsync(conn,
            "SELECT count(*) FROM pg_publication WHERE pubname = @p", default, ("p", pub))).IsEqualTo(1L);

        var plugin = await PgExec.ScalarStringAsync(conn,
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = @s", default, ("s", slot));
        await Assert.That(plugin).IsEqualTo("pgoutput");
        
        // 5 directly-mapped (categories, products, customers, sales.orders, sales.order_lines) plus
        // 2 from the skip-navigation (labels and the product_labels join table) — all in capture-all-mapped mode.
        await Assert.That(await PgExec.ScalarLongAsync(conn,
            "SELECT count(*) FROM pg_publication_tables WHERE pubname = @p AND schemaname IN ('public', 'sales')",
            default, ("p", pub))).IsEqualTo(7L);

        // State tables exist.
        foreach (var table in new[] { "wallaby.checkpoint", "wallaby.backfill_state", "wallaby.slot_registry" })
        {
            await Assert.That(await PgExec.ScalarStringAsync(conn,
                "SELECT to_regclass(@t)::text", default, ("t", table))).IsEqualTo(table);
        }

        // Slot registry row recorded.
        await Assert.That(await PgExec.ScalarLongAsync(conn,
            "SELECT count(*) FROM wallaby.slot_registry WHERE slot_name = @s", default, ("s", slot))).IsEqualTo(1L);
    }

    [Test]
    public async Task Re_running_is_idempotent()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        var pub = $"cdc_pub_{suffix}";
        var model = BuildAllMappedModel();

        var first = await CreateConfigurator(slot, pub).EnsureConfiguredAsync(model, CancellationToken.None);
        var second = await CreateConfigurator(slot, pub).EnsureConfiguredAsync(model, CancellationToken.None);

        await Assert.That(first.PublicationCreated).IsTrue();
        await Assert.That(first.SlotCreated).IsTrue();
        await Assert.That(second.PublicationCreated).IsFalse();
        await Assert.That(second.SlotCreated).IsFalse();
    }

    [Test]
    public async Task Provisions_external_slot_and_publication_without_consuming_it()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        var pub = $"cdc_pub_{suffix}";
        var extSlot = $"elt_slot_{suffix}";
        var extPub = $"elt_pub_{suffix}";
        var model = BuildAllMappedModel();

        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions
            {
                SlotName = slot,
                PublicationName = pub,
                ExternalSlots =
                [
                    new ExternalSlotSpec(extSlot, extPub, [("public", "products"), ("public", "customers")]),
                ],
            },
            NullLogger.Instance);

        try
        {
            var result = await configurator.EnsureConfiguredAsync(model, CancellationToken.None);

            await Assert.That(result.ExternalSlots.Count).IsEqualTo(1);
            await Assert.That(result.ExternalSlots[0].SlotCreated).IsTrue();
            await Assert.That(result.ExternalSlots[0].PublicationCreated).IsTrue();

            await using var conn = new NpgsqlConnection(pg.ConnectionString);
            await conn.OpenAsync();

            // The external publication exists and contains exactly the two declared tables.
            await Assert.That(await PgExec.ScalarLongAsync(conn,
                "SELECT count(*) FROM pg_publication WHERE pubname = @p", default, ("p", extPub))).IsEqualTo(1L);
            await Assert.That(await PgExec.ScalarLongAsync(conn,
                "SELECT count(*) FROM pg_publication_tables WHERE pubname = @p", default, ("p", extPub))).IsEqualTo(2L);

            // The external slot is pgoutput and NOT active — Wallaby provisions but never consumes it.
            await Assert.That(await PgExec.ScalarStringAsync(conn,
                "SELECT plugin FROM pg_replication_slots WHERE slot_name = @s", default, ("s", extSlot))).IsEqualTo("pgoutput");
            await Assert.That(await PgExec.ScalarBoolAsync(conn,
                "SELECT active FROM pg_replication_slots WHERE slot_name = @s", default, ("s", extSlot))).IsFalse();

            // The registry distinguishes external slots from the primary one.
            await Assert.That(await PgExec.ScalarStringAsync(conn,
                "SELECT kind FROM wallaby.slot_registry WHERE slot_name = @s", default, ("s", extSlot))).IsEqualTo("external");
            await Assert.That(await PgExec.ScalarStringAsync(conn,
                "SELECT kind FROM wallaby.slot_registry WHERE slot_name = @s", default, ("s", slot))).IsEqualTo("primary");
        }
        finally
        {
            await DropSlotsAndPublicationsAsync(slot, pub, extSlot, extPub);
        }
    }

    [Test]
    public async Task External_slot_is_idempotent_and_reconciles_tables()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        var pub = $"cdc_pub_{suffix}";
        var extSlot = $"elt_slot_{suffix}";
        var extPub = $"elt_pub_{suffix}";
        var model = BuildAllMappedModel();

        PostgresSelfConfigurator Make(params (string Schema, string Table)[] tables) => new(
            pg.DataSource,
            new SelfConfigOptions
            {
                SlotName = slot,
                PublicationName = pub,
                ExternalSlots = [new ExternalSlotSpec(extSlot, extPub, tables)],
            },
            NullLogger.Instance);

        try
        {
            var first = await Make(("public", "products"), ("public", "customers"))
                .EnsureConfiguredAsync(model, CancellationToken.None);
            await Assert.That(first.ExternalSlots[0].SlotCreated).IsTrue();
            await Assert.That(first.ExternalSlots[0].PublicationCreated).IsTrue();

            // Re-run with a changed table set: 'customers' dropped, 'categories' added; slot/pub reused.
            var second = await Make(("public", "products"), ("public", "categories"))
                .EnsureConfiguredAsync(model, CancellationToken.None);
            await Assert.That(second.ExternalSlots[0].SlotCreated).IsFalse();
            await Assert.That(second.ExternalSlots[0].PublicationCreated).IsFalse();

            await using var conn = new NpgsqlConnection(pg.ConnectionString);
            await conn.OpenAsync();
            var tables = new List<string>();
            await using (var cmd = new NpgsqlCommand(
                "SELECT tablename FROM pg_publication_tables WHERE pubname = @p", conn))
            {
                cmd.Parameters.AddWithValue("p", extPub);
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    tables.Add(reader.GetString(0));
                }
            }

            await Assert.That(tables.Contains("products")).IsTrue();
            await Assert.That(tables.Contains("categories")).IsTrue();
            await Assert.That(tables.Contains("customers")).IsFalse(); // reconciled away
        }
        finally
        {
            await DropSlotsAndPublicationsAsync(slot, pub, extSlot, extPub);
        }
    }

    [Test]
    public async Task Adopts_a_preexisting_pgoutput_slot_and_records_it()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        var pub = $"cdc_pub_{suffix}";
        var extSlot = $"elt_slot_{suffix}";
        var extPub = $"elt_pub_{suffix}";
        var model = BuildAllMappedModel();

        try
        {
            // A pgoutput logical slot already exists on the server, created outside Wallaby.
            await using (var seed = new NpgsqlConnection(pg.ConnectionString))
            {
                await seed.OpenAsync();
                await PgExec.ExecuteAsync(
                    seed, "SELECT pg_create_logical_replication_slot(@s, 'pgoutput')", default, ("s", extSlot));
            }

            var configurator = new PostgresSelfConfigurator(
                pg.DataSource,
                new SelfConfigOptions
                {
                    SlotName = slot,
                    PublicationName = pub,
                    ExternalSlots = [new ExternalSlotSpec(extSlot, extPub, [("public", "products")])],
                },
                NullLogger.Instance);

            var result = await configurator.EnsureConfiguredAsync(model, CancellationToken.None);

            // Adopted, not recreated...
            await Assert.That(result.ExternalSlots[0].SlotCreated).IsFalse();

            // ...and now recorded in the registry as external.
            await using var conn = new NpgsqlConnection(pg.ConnectionString);
            await conn.OpenAsync();
            await Assert.That(await PgExec.ScalarStringAsync(conn,
                "SELECT kind FROM wallaby.slot_registry WHERE slot_name = @s", default, ("s", extSlot))).IsEqualTo("external");
        }
        finally
        {
            await DropSlotsAndPublicationsAsync(slot, pub, extSlot, extPub);
        }
    }

    [Test]
    public async Task Rejects_a_preexisting_slot_with_the_wrong_type()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        var pub = $"cdc_pub_{suffix}";
        var extSlot = $"elt_slot_{suffix}";
        var extPub = $"elt_pub_{suffix}";
        var model = BuildAllMappedModel();

        try
        {
            // A physical slot squats the declared name — not a pgoutput logical slot.
            await using (var seed = new NpgsqlConnection(pg.ConnectionString))
            {
                await seed.OpenAsync();
                await PgExec.ExecuteAsync(
                    seed, "SELECT pg_create_physical_replication_slot(@s)", default, ("s", extSlot));
            }

            var configurator = new PostgresSelfConfigurator(
                pg.DataSource,
                new SelfConfigOptions
                {
                    SlotName = slot,
                    PublicationName = pub,
                    ExternalSlots = [new ExternalSlotSpec(extSlot, extPub, [("public", "products")])],
                },
                NullLogger.Instance);

            await Assert.That(async () => await configurator.EnsureConfiguredAsync(model, CancellationToken.None))
                .Throws<CdcConfigurationException>();
        }
        finally
        {
            await DropSlotsAndPublicationsAsync(slot, pub, extSlot, extPub);
        }
    }

    // External slots are never auto-dropped by Wallaby, so tests must clean up their own to avoid
    // exhausting max_replication_slots in the shared session database.
    private async Task DropSlotsAndPublicationsAsync(string slot, string pub, string extSlot, string extPub)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();

        foreach (var slotName in new[] { slot, extSlot })
        {
            await PgExec.ExecuteAsync(
                conn,
                "SELECT pg_drop_replication_slot(@s) WHERE EXISTS " +
                "(SELECT 1 FROM pg_replication_slots WHERE slot_name = @s AND NOT active)",
                default,
                ("s", slotName));
        }

        foreach (var publication in new[] { pub, extPub })
        {
            await PgExec.ExecuteAsync(conn, $"DROP PUBLICATION IF EXISTS {PgExec.QuoteIdentifier(publication)}", default);
        }
    }

    [Test]
    public async Task Wrong_wal_level_fails_fast()
    {
        // A plain Postgres (default wal_level = replica) should be rejected with guidance.
        await using var plain = new PostgreSqlBuilder("postgres:17").Build();
        await plain.StartAsync();
        try
        {
            await using var plainSource = NpgsqlDataSource.Create(plain.GetConnectionString());
            var configurator = new PostgresSelfConfigurator(
                plainSource,
                new SelfConfigOptions { SlotName = "x", PublicationName = "y" },
                NullLogger.Instance);

            await Assert.That(async () => await configurator.EnsureConfiguredAsync(BuildAllMappedModel(), CancellationToken.None))
                .Throws<CdcConfigurationException>();
        }
        finally
        {
            await plain.DisposeAsync();
        }
    }
}
