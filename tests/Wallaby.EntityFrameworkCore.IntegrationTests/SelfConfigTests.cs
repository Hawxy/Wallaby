using System.Linq.Expressions;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;
using Wallaby.EntityFrameworkCore.Internal;
using Wallaby.Internal;
using Wallaby.Internal.SelfConfig;
using Wallaby.Model;
using Wallaby.Providers;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.EntityFrameworkCore.IntegrationTests;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class SelfConfigTests(TestModelPostgresFixture pg)
{
    private static WallabyModel BuildTestModel()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        Expression<Func<Product, List<Label>>> labels = p => p.Labels;
        return EfCoreCaptureModelBuilder.Build(ctx.Model, new CaptureSpec
        {
            DeclaredEntities = [typeof(Category), typeof(Product), typeof(Customer), typeof(Order), typeof(OrderLine), typeof(Label)],
            DeclaredDependencies = new Dictionary<Type, IReadOnlyList<LambdaExpression>>
            {
                [typeof(Product)] = [labels],
            },
        });
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
        var model = BuildTestModel();

        var result = await CreateConfigurator(slot, pub).EnsureConfiguredAsync(model, CancellationToken.None);

        result.PublicationCreated.ShouldBeTrue();
        result.SlotCreated.ShouldBeTrue();
        result.ConsistentPoint.ShouldNotBeNull();

        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();

        (await PgExec.ScalarLongAsync(conn,
            "SELECT count(*) FROM pg_publication WHERE pubname = @p", default, ("p", pub))).ShouldBe(1L);

        var plugin = await PgExec.ScalarStringAsync(conn,
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = @s", default, ("s", slot));
        plugin.ShouldBe("pgoutput");

        // 6 declared (categories, products, customers, labels, sales.orders, sales.order_lines) plus
        // the product_labels join table pulled in by the Labels DependsOn.
        (await PgExec.ScalarLongAsync(conn,
            "SELECT count(*) FROM pg_publication_tables WHERE pubname = @p AND schemaname IN ('public', 'sales')",
            default, ("p", pub))).ShouldBe(7L);

        // State tables exist.
        foreach (var table in new[] { "wallaby.checkpoint", "wallaby.backfill_state", "wallaby.slot_registry", "wallaby.fanout_queue" })
        {
            (await PgExec.ScalarStringAsync(conn,
                "SELECT to_regclass(@t)::text", default, ("t", table))).ShouldBe(table);
        }

        // Due-job index on the fan-out queue exists.
        (await PgExec.ScalarLongAsync(conn,
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'wallaby' AND indexname = 'fanout_queue_due_idx'",
            default)).ShouldBe(1L);

        // Slot registry row recorded.
        (await PgExec.ScalarLongAsync(conn,
            "SELECT count(*) FROM wallaby.slot_registry WHERE slot_name = @s", default, ("s", slot))).ShouldBe(1L);
    }

    [Test]
    public async Task Re_running_is_idempotent()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        var pub = $"cdc_pub_{suffix}";
        var model = BuildTestModel();

        var first = await CreateConfigurator(slot, pub).EnsureConfiguredAsync(model, CancellationToken.None);
        var second = await CreateConfigurator(slot, pub).EnsureConfiguredAsync(model, CancellationToken.None);

        first.PublicationCreated.ShouldBeTrue();
        first.SlotCreated.ShouldBeTrue();
        second.PublicationCreated.ShouldBeFalse();
        second.SlotCreated.ShouldBeFalse();
    }

    [Test]
    public async Task Provisions_external_slot_and_publication_without_consuming_it()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        var pub = $"cdc_pub_{suffix}";
        var extSlot = $"elt_slot_{suffix}";
        var extPub = $"elt_pub_{suffix}";
        var model = BuildTestModel();

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

            result.ExternalSlots.Count.ShouldBe(1);
            result.ExternalSlots[0].SlotCreated.ShouldBeTrue();
            result.ExternalSlots[0].PublicationCreated.ShouldBeTrue();

            await using var conn = new NpgsqlConnection(pg.ConnectionString);
            await conn.OpenAsync();

            // The external publication exists and contains exactly the two declared tables.
            (await PgExec.ScalarLongAsync(conn,
                "SELECT count(*) FROM pg_publication WHERE pubname = @p", default, ("p", extPub))).ShouldBe(1L);
            (await PgExec.ScalarLongAsync(conn,
                "SELECT count(*) FROM pg_publication_tables WHERE pubname = @p", default, ("p", extPub))).ShouldBe(2L);

            // The external slot is pgoutput and NOT active — Wallaby provisions but never consumes it.
            (await PgExec.ScalarStringAsync(conn,
                "SELECT plugin FROM pg_replication_slots WHERE slot_name = @s", default, ("s", extSlot))).ShouldBe("pgoutput");
            (await PgExec.ScalarBoolAsync(conn,
                "SELECT active FROM pg_replication_slots WHERE slot_name = @s", default, ("s", extSlot))).ShouldBeFalse();

            // The registry distinguishes external slots from the primary one.
            (await PgExec.ScalarStringAsync(conn,
                "SELECT kind FROM wallaby.slot_registry WHERE slot_name = @s", default, ("s", extSlot))).ShouldBe("external");
            (await PgExec.ScalarStringAsync(conn,
                "SELECT kind FROM wallaby.slot_registry WHERE slot_name = @s", default, ("s", slot))).ShouldBe("primary");
        }
        finally
        {
            await DropSlotsAndPublicationsAsync(slot, pub, extSlot, extPub);
        }
    }

    [Test]
    public async Task Provision_only_creates_external_slot_without_a_primary()
    {
        var suffix = Guid.NewGuid().ToString("N");
        // Primary names are supplied but must be IGNORED by the provision-only path (proves no primary slot).
        var primarySlot = $"cdc_primary_{suffix}";
        var primaryPub = $"cdc_primary_pub_{suffix}";
        var extSlot = $"elt_slot_{suffix}";
        var extPub = $"elt_pub_{suffix}";

        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions
            {
                SlotName = primarySlot,
                PublicationName = primaryPub,
                ExternalSlots = [new ExternalSlotSpec(extSlot, extPub, [("public", "products"), ("public", "customers")])],
            },
            NullLogger.Instance);

        try
        {
            var first = await configurator.EnsureExternalSlotsOnlyAsync(CancellationToken.None);
            first.Count.ShouldBe(1);
            first[0].SlotCreated.ShouldBeTrue();
            first[0].PublicationCreated.ShouldBeTrue();

            await using var conn = new NpgsqlConnection(pg.ConnectionString);
            await conn.OpenAsync();

            // External publication + slot exist; the slot is pgoutput and unconsumed; registry marks it external.
            (await PgExec.ScalarLongAsync(conn,
                "SELECT count(*) FROM pg_publication_tables WHERE pubname = @p", default, ("p", extPub))).ShouldBe(2L);
            (await PgExec.ScalarStringAsync(conn,
                "SELECT plugin FROM pg_replication_slots WHERE slot_name = @s", default, ("s", extSlot))).ShouldBe("pgoutput");
            (await PgExec.ScalarBoolAsync(conn,
                "SELECT active FROM pg_replication_slots WHERE slot_name = @s", default, ("s", extSlot))).ShouldBeFalse();
            (await PgExec.ScalarStringAsync(conn,
                "SELECT kind FROM wallaby.slot_registry WHERE slot_name = @s", default, ("s", extSlot))).ShouldBe("external");

            // The primary slot/publication were NOT created — provision-only never touches them.
            (await PgExec.ScalarLongAsync(conn,
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = @s", default, ("s", primarySlot))).ShouldBe(0L);
            (await PgExec.ScalarLongAsync(conn,
                "SELECT count(*) FROM pg_publication WHERE pubname = @p", default, ("p", primaryPub))).ShouldBe(0L);

            // Idempotent re-run.
            var second = await configurator.EnsureExternalSlotsOnlyAsync(CancellationToken.None);
            second[0].SlotCreated.ShouldBeFalse();
            second[0].PublicationCreated.ShouldBeFalse();
        }
        finally
        {
            await DropSlotsAndPublicationsAsync(primarySlot, primaryPub, extSlot, extPub);
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
        var model = BuildTestModel();

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
            first.ExternalSlots[0].SlotCreated.ShouldBeTrue();
            first.ExternalSlots[0].PublicationCreated.ShouldBeTrue();

            // Re-run with a changed table set: 'customers' dropped, 'categories' added; slot/pub reused.
            var second = await Make(("public", "products"), ("public", "categories"))
                .EnsureConfiguredAsync(model, CancellationToken.None);
            second.ExternalSlots[0].SlotCreated.ShouldBeFalse();
            second.ExternalSlots[0].PublicationCreated.ShouldBeFalse();

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

            tables.Contains("products").ShouldBeTrue();
            tables.Contains("categories").ShouldBeTrue();
            tables.Contains("customers").ShouldBeFalse(); // reconciled away
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
        var model = BuildTestModel();

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
            result.ExternalSlots[0].SlotCreated.ShouldBeFalse();

            // ...and now recorded in the registry as external.
            await using var conn = new NpgsqlConnection(pg.ConnectionString);
            await conn.OpenAsync();
            (await PgExec.ScalarStringAsync(conn,
                "SELECT kind FROM wallaby.slot_registry WHERE slot_name = @s", default, ("s", extSlot))).ShouldBe("external");
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
        var model = BuildTestModel();

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

            await Should.ThrowAsync<WallabyConfigurationException>(
                async () => await configurator.EnsureConfiguredAsync(model, CancellationToken.None));
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

            await Should.ThrowAsync<WallabyConfigurationException>(
                async () => await configurator.EnsureConfiguredAsync(BuildTestModel(), CancellationToken.None));
        }
        finally
        {
            await plain.DisposeAsync();
        }
    }
}
