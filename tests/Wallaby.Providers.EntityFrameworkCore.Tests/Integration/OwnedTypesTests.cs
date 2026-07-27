using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Internal.SelfConfig;
using Wallaby.Providers;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Same-table owned references and complex properties flow end to end: live changes and backfills
/// deliver their values to transforms, and a column selection naming an owned navigation publishes
/// its columns in the publication column list.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class OwnedTypesTests(TestModelPostgresFixture pg)
{
    private static Supplier NewSupplier(string name, string street, double lat = 1.5, double lon = 2.5) => new()
    {
        Name = name,
        Address = new Address { Street = street, City = "Springfield", Location = new GeoPoint(lat, lon) },
        Contact = new ContactCard($"{name}@example.com", "555-0100"),
    };

    private static WallabyDocument Project(Supplier s) => new()
    {
        ["name"] = s.Name,
        ["street"] = s.Address.Street,
        ["lat"] = s.Address.Location?.Lat,
        ["email"] = s.Contact.Email,
        ["billingCity"] = s.BillingAddress?.City,
    };

    [Test]
    public async Task Live_changes_deliver_owned_and_complex_values()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<Supplier>("capture", "suppliers", Project);
        await harness.SelfConfigureAsync();

        await harness.Db.AddSupplierAsync(NewSupplier("live_owned", "1 Main St"));

        await harness.RunUntilAsync(
            () => capture.For("suppliers").Any(r => r.Document?["name"] as string == "live_owned"),
            timeout: TimeSpan.FromSeconds(30));

        var document = capture.For("suppliers").Last(r => r.Document?["name"] as string == "live_owned").Document!;
        document["street"].ShouldBe("1 Main St");
        document["lat"].ShouldBe(1.5);
        document["email"].ShouldBe("live_owned@example.com");
        document["billingCity"].ShouldBeNull();
    }

    [Test]
    public async Task A_backfill_delivers_owned_and_complex_values()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var version = harness.Names.Suffix; // unique => isolates the shared backfill_state row
        var capture = harness.AddCaptureSink();
        harness.Project<Supplier>("capture", "suppliers", Project, backfill: true, backfillVersion: version);

        await harness.Db.AddSupplierAsync(NewSupplier("backfill_owned", "7 Quay Rd", lat: 3.25, lon: 4.75));

        await harness.SelfConfigureAsync();
        await harness.StartAsync();
        await harness.RunBackfillAsync(version);

        var document = capture.For("suppliers")
            .Last(r => r.Document?["name"] as string == "backfill_owned").Document!;
        document["street"].ShouldBe("7 Quay Rd");
        document["lat"].ShouldBe(3.25);
        document["email"].ShouldBe("backfill_owned@example.com");
    }

    [Test]
    public async Task A_consumed_owned_navigation_publishes_its_columns()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, new CaptureSpec
        {
            DeclaredEntities = [typeof(Supplier)],
            DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
            {
                [typeof(Supplier)] =
                [
                    new ColumnSelection(
                        ColumnSelectionMode.Include, [nameof(Supplier.Name), nameof(Supplier.Address)]),
                ],
            },
        });

        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions
            {
                SlotName = names.Slot,
                PublicationName = names.Publication,
                PublicationColumnLists = true,
                ManagePublicationTables = true,
            },
            NullLogger.Instance);
        await configurator.EnsureConfiguredAsync(model, CancellationToken.None);

        var columns = await ReadPublicationColumnsAsync(names.Publication, "suppliers");
        columns.ShouldNotBeNull();
        columns!.ShouldBe(
            ["Id", "Name", "address_street", "address_city", "address_lat", "address_lon"],
            ignoreOrder: true);
    }

    private async Task<HashSet<string>?> ReadPublicationColumnsAsync(string publication, string table)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            """
            SELECT CASE WHEN pr.prattrs IS NULL THEN NULL
                        ELSE (SELECT array_agg(a.attname::text)
                              FROM pg_attribute a
                              WHERE a.attrelid = pr.prrelid AND a.attnum = ANY (pr.prattrs))
                   END AS columns
            FROM pg_publication p
            JOIN pg_publication_rel pr ON pr.prpubid = p.oid
            JOIN pg_class c ON c.oid = pr.prrelid
            WHERE p.pubname = @p AND c.relname = @t
            """,
            conn);
        cmd.Parameters.AddWithValue("p", publication);
        cmd.Parameters.AddWithValue("t", table);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync() || reader.IsDBNull(0))
        {
            return null;
        }
        return new HashSet<string>(reader.GetFieldValue<string[]>(0), StringComparer.Ordinal);
    }
}
