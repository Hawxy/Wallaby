using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Internal;
using Wallaby.Internal.SelfConfig;
using Wallaby.Model;
using Wallaby.Providers;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Declaratively partitioned tables: every managed publication publishes via the partition root, so
/// changes on leaf partitions stream under the root's name and the whole pipeline (routing, backfill,
/// purge) treats the table as one relation.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class PartitionedTablesTests(TestModelPostgresFixture pg)
{
    [Test]
    public async Task Publications_are_created_with_publish_via_partition_root()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var extSlot = names.Named("elt_slot");
        var extPub = names.Named("elt_pub");
        names.TrackExternal(extSlot, extPub);

        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions
            {
                SlotName = names.Slot,
                PublicationName = names.Publication,
                ExternalSlots = [new ExternalSlotSpec(extSlot, extPub, [("public", "products")])],
            },
            NullLogger.Instance);

        await configurator.EnsureConfiguredAsync(BuildProductModel(), CancellationToken.None);

        (await ReadViaRootAsync(names.Publication)).ShouldBe(true);
        (await ReadViaRootAsync(extPub)).ShouldBe(true);
    }

    [Test]
    public async Task An_existing_publication_without_the_parameter_gets_it_enabled()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await ExecAsync($"CREATE PUBLICATION {names.Publication} FOR TABLE products");
        (await ReadViaRootAsync(names.Publication)).ShouldBe(false);

        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions { SlotName = names.Slot, PublicationName = names.Publication },
            NullLogger.Instance);
        await configurator.EnsureConfiguredAsync(BuildProductModel(), CancellationToken.None);

        (await ReadViaRootAsync(names.Publication)).ShouldBe(true);
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        (await PgExec.ScalarLongAsync(conn,
            "SELECT count(*) FROM pg_publication_tables WHERE pubname = @p AND tablename = 'products'",
            default, ("p", names.Publication))).ShouldBe(1L);
    }

    [Test]
    public async Task An_unmanaged_publication_without_the_parameter_throws_for_a_partitioned_capture()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await CreateLedgerTableAsync();
        await ExecAsync($"CREATE PUBLICATION {names.Publication} FOR TABLE ledger_entries");

        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions
            {
                SlotName = names.Slot,
                PublicationName = names.Publication,
                ManagePublicationTables = false,
            },
            NullLogger.Instance);

        var ex = await Should.ThrowAsync<WallabyConfigurationException>(
            () => configurator.EnsureConfiguredAsync(BuildLedgerModel(), CancellationToken.None));
        ex.Message.ShouldContain("public.ledger_entries");
        ex.Message.ShouldContain("publish_via_partition_root");
    }

    [Test]
    public async Task Replica_identity_validation_covers_every_leaf_partition()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await CreateLedgerTableAsync();

        PostgresSelfConfigurator Make() => new(
            pg.DataSource,
            new SelfConfigOptions
            {
                SlotName = names.Slot,
                PublicationName = names.Publication,
                RequireFullReplicaIdentity = true,
            },
            NullLogger.Instance);
        var model = BuildLedgerModel(requiresFullReplicaIdentity: true);

        // The root's identity does not propagate, so both default-identity leaves fail the check.
        var ex = await Should.ThrowAsync<WallabyConfigurationException>(
            () => Make().EnsureConfiguredAsync(model, CancellationToken.None));
        ex.Message.ShouldContain("ledger_entries_p0");
        ex.Message.ShouldContain("ledger_entries_p1");

        await ExecAsync("ALTER TABLE ledger_entries_p0 REPLICA IDENTITY FULL");
        await ExecAsync("ALTER TABLE ledger_entries_p1 REPLICA IDENTITY FULL");
        await Make().EnsureConfiguredAsync(model, CancellationToken.None);
    }

    [Test]
    public async Task Changes_on_leaves_stream_under_the_root_and_purge_backfill_spans_partitions()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await CreateLedgerTableAsync();
        var capture = new CaptureSink();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<LedgerContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc => cdc
            .UseEntityFrameworkCore<LedgerContext>()
            .UseConnectionString(pg.ConnectionString)
            .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
            .WithMappings(sink => sink.Map<LedgerEntry>()
                .ToDestination("ledger")
                .UsingTransform(LedgerNames)));
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
        });
        services.ReplaceWallabySink("capture", capture);

        await using var node = await WallabyTestNode.StartAsync(services);
        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        // One row in each leaf; both must be delivered under the root mapping via live streaming.
        await ExecAsync("""INSERT INTO ledger_entries ("Id", "Name") VALUES (1, 'low'), (1500, 'high')""");
        await capture.WaitForDocumentsAsync(["1", "1500"]);

        await ExecAsync("""UPDATE ledger_entries SET "Name" = 'low_v2' WHERE "Id" = 1""");
        await ExecAsync("""DELETE FROM ledger_entries WHERE "Id" = 1500""");
        await capture.WaitForAsync(records =>
            records.Any(r => r.DocumentId == "1500" && r.IsDeletion) &&
            records.Any(r => r.DocumentId == "1" && r.Document?["name"]?.ToString() == "low_v2"));

        // A purge backfill snapshots the root, which spans every partition.
        await node.Services.GetRequiredService<IWallabyBackfillManager>()
            .RequestBackfillAsync<LedgerEntry>(purge: true);
        await capture.WaitForAsync(records =>
            capture.Purges.Count > 0 &&
            records.Any(r => r.DocumentId == "1" && r.Metadata.IsBackfill));

        var latest = capture.LatestByDocumentId("ledger");
        latest.ShouldContainKey("1");
        latest.ShouldNotContainKey("1500");
    }

    private static Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> LedgerNames(
        DbContext db, IReadOnlyList<ChangeEvent<LedgerEntry>> changes, CancellationToken ct)
    {
        var docs = new Dictionary<DocumentKey, WallabyDocument?>();
        foreach (var c in changes)
        {
            docs[c.Key] = new WallabyDocument { ["name"] = c.Entity!.Name };
        }
        return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(docs);
    }

    private static WallabyModel BuildProductModel()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        return EfCoreCaptureModelBuilder.Build(ctx.Model, new CaptureSpec { DeclaredEntities = [typeof(Product)] });
    }

    private WallabyModel BuildLedgerModel(bool requiresFullReplicaIdentity = false)
    {
        var options = new DbContextOptionsBuilder<LedgerContext>().UseNpgsql(pg.ConnectionString).Options;
        using var ctx = new LedgerContext(options);
        return EfCoreCaptureModelBuilder.Build(ctx.Model, new CaptureSpec
        {
            DeclaredEntities = [typeof(LedgerEntry)],
            RequiresFullReplicaIdentity = requiresFullReplicaIdentity
                ? new HashSet<Type> { typeof(LedgerEntry) }
                : new HashSet<Type>(),
        });
    }

    // The partition key must be part of the primary key, so the table ranges over its own PK.
    private async Task CreateLedgerTableAsync()
    {
        await ExecAsync("DROP TABLE IF EXISTS ledger_entries CASCADE");
        await ExecAsync(
            """
            CREATE TABLE ledger_entries (
                "Id"   integer NOT NULL,
                "Name" text    NOT NULL,
                PRIMARY KEY ("Id")
            ) PARTITION BY RANGE ("Id");
            CREATE TABLE ledger_entries_p0 PARTITION OF ledger_entries FOR VALUES FROM (0) TO (1000);
            CREATE TABLE ledger_entries_p1 PARTITION OF ledger_entries FOR VALUES FROM (1000) TO (2000);
            """);
    }

    private async Task<bool> ReadViaRootAsync(string publication)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        return await PgExec.ScalarBoolAsync(
            conn, "SELECT pubviaroot FROM pg_publication WHERE pubname = @p", default, ("p", publication));
    }

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }
}

/// <summary>An entity whose physical table is range-partitioned over its primary key.</summary>
public class LedgerEntry
{
    public int Id { get; set; }

    public string Name { get; set; } = "";
}

public class LedgerContext(DbContextOptions<LedgerContext> options) : DbContext(options)
{
    public DbSet<LedgerEntry> Entries => Set<LedgerEntry>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.Entity<LedgerEntry>(e =>
        {
            e.ToTable("ledger_entries");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).ValueGeneratedNever();
        });
}
