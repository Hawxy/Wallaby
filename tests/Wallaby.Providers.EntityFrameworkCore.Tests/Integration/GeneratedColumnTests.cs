using Microsoft.EntityFrameworkCore;
using Npgsql;
using Testcontainers.PostgreSql;
using Wallaby.Internal;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.TestInfrastructure;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Stored generated columns (EF <c>HasComputedColumnSql(..., stored: true)</c>) are publishable only from
/// PostgreSQL 18. Older servers never put them on the wire, so a captured one would materialize as its
/// default on live changes while backfill reads the real value: self-config must refuse it. On PG18+
/// the managed publication sets <c>publish_generated_columns = stored</c> and the value streams.
/// </summary>
[NotInParallel]
public class GeneratedColumnTests
{
    public sealed class Gadget
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public string NameUpper { get; set; } = "";
    }

    public sealed class GadgetContext(DbContextOptions<GadgetContext> options) : DbContext(options)
    {
        public DbSet<Gadget> Gadgets => Set<Gadget>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            var gadget = modelBuilder.Entity<Gadget>().ToTable("gadgets");
            gadget.Property(g => g.Id).HasColumnName("id");
            gadget.Property(g => g.Name).HasColumnName("name");
            gadget.Property(g => g.NameUpper).HasColumnName("name_upper").HasComputedColumnSql("upper(name)", stored: true);
        }
    }

    private static DbContextOptions<GadgetContext> Options(string connectionString)
        => new DbContextOptionsBuilder<GadgetContext>().UseNpgsql(connectionString).Options;

    private static WallabyTestHarness Harness(string connectionString)
    {
        var contextFactory = () => new GadgetContext(Options(connectionString));
        using var context = contextFactory();
        return new WallabyTestHarness(connectionString, new EfCoreModelProvider(context.Model))
            .UseEnrichmentSessions(new DbContextEnrichmentSessionProvider(contextFactory))
            .Broadcast()
            .Capture<Gadget>();
    }

    [Test]
    public async Task Before_postgres_18_a_captured_stored_generated_column_fails_fast_and_exclusion_is_the_remedy()
    {
        await using var container = new PostgreSqlBuilder(PostgresImages.Postgres17)
            .WithCommand("-c", "wal_level=logical")
            .Build();
        await container.StartAsync();
        var connectionString = container.GetConnectionString();
        await using (var ctx = new GadgetContext(Options(connectionString)))
        {
            await ctx.Database.EnsureCreatedAsync();
        }

        await using (var harness = Harness(connectionString))
        {
            harness.AddCaptureSink();
            var ex = await Should.ThrowAsync<WallabyConfigurationException>(() => harness.SelfConfigureAsync());
            ex.Message.ShouldContain("public.gadgets.name_upper");
            ex.Message.ShouldContain("ConsumesAllExcept");
        }

        // Excluding the property from capture is the documented remedy.
        await using (var harness = Harness(connectionString).ConsumesAllExcept<Gadget>(nameof(Gadget.NameUpper)))
        {
            harness.AddCaptureSink();
            await harness.SelfConfigureAsync();
        }
    }

    [Test]
    public async Task Postgres_18_and_later_stream_stored_generated_columns()
    {
        await using var container = new PostgreSqlBuilder(PostgresImages.Postgres19).Build();
        await container.StartAsync();
        var connectionString = container.GetConnectionString();
        await using (var ctx = new GadgetContext(Options(connectionString)))
        {
            await ctx.Database.EnsureCreatedAsync();
        }

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        // Whole-table member: the managed publication's publish_generated_columns = stored carries it.
        await using (var harness = Harness(connectionString))
        {
            var capture = harness.AddCaptureSink();
            await harness.SelfConfigureAsync();
            await InsertGadgetAsync(connectionString, "widget");

            await harness.RunUntilAsync(() => capture.For("gadgets").Any(r => r.Document is not null));
            capture.For("gadgets").First(r => r.Document is not null).Document!["NameUpper"].ShouldBe("WIDGET");
            (await PgExec.ScalarStringAsync(conn,
                "SELECT pubgencols::text FROM pg_publication WHERE pubname = @p", default, ("p", harness.Names.Publication)))
                .ShouldBe("s");
        }

        // Narrowed member: the column list names the generated column explicitly.
        await using (var harness = Harness(connectionString)
            .Consumes<Gadget>(nameof(Gadget.Id), nameof(Gadget.Name), nameof(Gadget.NameUpper)))
        {
            var capture = harness.AddCaptureSink();
            await harness.SelfConfigureAsync();
            await InsertGadgetAsync(connectionString, "gizmo");

            await harness.RunUntilAsync(() => capture.For("gadgets").Any(r => Equals(r.Document?["Name"], "gizmo")));
            capture.For("gadgets").First(r => Equals(r.Document?["Name"], "gizmo")).Document!["NameUpper"].ShouldBe("GIZMO");
            (await PgExec.ScalarStringAsync(conn,
                "SELECT array_to_string(attnames, ',') FROM pg_publication_tables WHERE pubname = @p AND tablename = 'gadgets'",
                default, ("p", harness.Names.Publication)))
                .ShouldBe("id,name,name_upper");
        }
    }

    private static async Task InsertGadgetAsync(string connectionString, string name)
    {
        await using var ctx = new GadgetContext(Options(connectionString));
        ctx.Gadgets.Add(new Gadget { Name = name });
        await ctx.SaveChangesAsync();
    }
}
