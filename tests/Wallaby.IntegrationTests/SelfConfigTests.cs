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
