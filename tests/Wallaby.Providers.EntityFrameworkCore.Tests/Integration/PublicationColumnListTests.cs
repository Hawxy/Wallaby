using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;
using Wallaby.Abstractions;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Internal;
using Wallaby.Internal.SelfConfig;
using Wallaby.Model;
using Wallaby.Providers;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class PublicationColumnListTests(TestModelPostgresFixture pg)
{
    private static WallabyModel BuildTestModel(
        IReadOnlyDictionary<Type, IReadOnlyList<ColumnSelection>>? selections = null,
        IReadOnlySet<Type>? requiresFullReplicaIdentity = null)
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        return EfCoreCaptureModelBuilder.Build(ctx.Model, new CaptureSpec
        {
            DeclaredEntities = [typeof(Category), typeof(Product), typeof(Customer)],
            RequiresFullReplicaIdentity = requiresFullReplicaIdentity ?? new HashSet<Type>(),
            DeclaredColumnSelections = selections ?? new Dictionary<Type, IReadOnlyList<ColumnSelection>>(),
        });
    }

    private static Dictionary<Type, IReadOnlyList<ColumnSelection>> Selection<TEntity>(
        ColumnSelectionMode mode, params string[] propertyNames) => new()
    {
        [typeof(TEntity)] = [new ColumnSelection(mode, propertyNames)],
    };

    private PostgresSelfConfigurator CreateConfigurator(
        WallabyNames names, bool columnLists = true, bool manageTables = true) =>
        new(pg.DataSource,
            new SelfConfigOptions
            {
                SlotName = names.Slot,
                PublicationName = names.Publication,
                PublicationColumnLists = columnLists,
                ManagePublicationTables = manageTables,
            },
            NullLogger.Instance);

    // prattrs is the source of truth: pg_publication_tables.attnames expands whole-table members to
    // all columns and cannot distinguish them from an explicit all-columns list.
    private async Task<Dictionary<string, HashSet<string>?>> ReadPublicationColumnsAsync(string pub)
    {
        var result = new Dictionary<string, HashSet<string>?>();
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            """
            SELECT c.relname,
                   CASE WHEN pr.prattrs IS NULL THEN NULL
                        ELSE (SELECT array_agg(a.attname::text)
                              FROM pg_attribute a
                              WHERE a.attrelid = pr.prrelid AND a.attnum = ANY (pr.prattrs))
                   END AS columns
            FROM pg_publication p
            JOIN pg_publication_rel pr ON pr.prpubid = p.oid
            JOIN pg_class c ON c.oid = pr.prrelid
            WHERE p.pubname = @p
            """,
            conn);
        cmd.Parameters.AddWithValue("p", pub);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result[reader.GetString(0)] = reader.IsDBNull(1)
                ? null
                : new HashSet<string>(reader.GetFieldValue<string[]>(1), StringComparer.Ordinal);
        }
        return result;
    }

    [Test]
    public async Task Publication_is_created_with_column_lists_matching_the_model()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var model = BuildTestModel(Selection<Product>(ColumnSelectionMode.Exclude, nameof(Product.Description)));

        await CreateConfigurator(names).EnsureConfiguredAsync(model, CancellationToken.None);

        var columns = await ReadPublicationColumnsAsync(names.Publication);
        columns.Count.ShouldBe(3);
        columns.Values.ShouldAllBe(c => c != null); // every member has an explicit list

        var expectedProduct = model.FindByClrType(typeof(Product))!.Columns.Select(c => c.ColumnName).ToHashSet();
        columns["products"]!.ShouldBe(expectedProduct, ignoreOrder: true);
        columns["products"]!.ShouldNotContain(nameof(Product.Description));
    }

    [Test]
    public async Task Reconcile_applies_column_lists_to_an_existing_whole_table_publication()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var model = BuildTestModel();

        // The upgrade path: a publication created without lists gains them on the next startup.
        await CreateConfigurator(names, columnLists: false).EnsureConfiguredAsync(model, CancellationToken.None);
        (await ReadPublicationColumnsAsync(names.Publication)).Values.ShouldAllBe(c => c == null);

        await CreateConfigurator(names, columnLists: true).EnsureConfiguredAsync(model, CancellationToken.None);
        (await ReadPublicationColumnsAsync(names.Publication)).Values.ShouldAllBe(c => c != null);
    }

    [Test]
    public async Task Column_list_drift_is_reconciled()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);

        await CreateConfigurator(names).EnsureConfiguredAsync(BuildTestModel(), CancellationToken.None);
        (await ReadPublicationColumnsAsync(names.Publication))["products"]!
            .ShouldContain(nameof(Product.Description));

        var excluded = BuildTestModel(Selection<Product>(ColumnSelectionMode.Exclude, nameof(Product.Description)));
        await CreateConfigurator(names).EnsureConfiguredAsync(excluded, CancellationToken.None);

        var columns = await ReadPublicationColumnsAsync(names.Publication);
        columns.Count.ShouldBe(3); // membership intact
        columns["products"]!.ShouldNotContain(nameof(Product.Description));
    }

    [Test]
    public async Task Consumes_selection_lists_only_named_columns_plus_the_primary_key()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var model = BuildTestModel(Selection<Product>(ColumnSelectionMode.Include, nameof(Product.Name)));

        await CreateConfigurator(names).EnsureConfiguredAsync(model, CancellationToken.None);

        (await ReadPublicationColumnsAsync(names.Publication))["products"]!
            .ShouldBe(["Id", "Name"], ignoreOrder: true);
    }

    [Test]
    public async Task Selections_from_multiple_mappings_union_in_the_publication_list()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var model = BuildTestModel(new Dictionary<Type, IReadOnlyList<ColumnSelection>>
        {
            [typeof(Product)] =
            [
                new ColumnSelection(ColumnSelectionMode.Include, [nameof(Product.Name)]),
                new ColumnSelection(ColumnSelectionMode.Include, [nameof(Product.Price)]),
            ],
        });

        await CreateConfigurator(names).EnsureConfiguredAsync(model, CancellationToken.None);

        (await ReadPublicationColumnsAsync(names.Publication))["products"]!
            .ShouldBe(["Id", "Name", "Price"], ignoreOrder: true);
    }

    [Test]
    public async Task Disabling_the_option_removes_column_lists()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var model = BuildTestModel();

        await CreateConfigurator(names, columnLists: true).EnsureConfiguredAsync(model, CancellationToken.None);
        await CreateConfigurator(names, columnLists: false).EnsureConfiguredAsync(model, CancellationToken.None);

        (await ReadPublicationColumnsAsync(names.Publication)).Values.ShouldAllBe(c => c == null);
    }

    [Test]
    public async Task Replica_identity_full_table_publishes_whole_rows()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var model = BuildTestModel();

        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await PgExec.ExecuteAsync(conn, "ALTER TABLE public.categories REPLICA IDENTITY FULL", default);
        try
        {
            var result = await CreateConfigurator(names).EnsureConfiguredAsync(model, CancellationToken.None);

            var columns = await ReadPublicationColumnsAsync(names.Publication);
            columns["categories"].ShouldBeNull();       // demoted: a list cannot cover identity FULL
            columns["products"].ShouldNotBeNull();      // others unaffected
            result.Warnings.ShouldContain(w => w.Contains("categories") && w.Contains("REPLICA IDENTITY FULL"));
        }
        finally
        {
            await PgExec.ExecuteAsync(conn, "ALTER TABLE public.categories REPLICA IDENTITY DEFAULT", default);
        }
    }

    [Test]
    public async Task Requires_full_replica_identity_table_is_never_listed()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        // Flagged tables are exempt even while relreplident is still 'd': the user is being told to
        // flip to FULL, and a list would turn that flip into publisher-side DML errors.
        var model = BuildTestModel(requiresFullReplicaIdentity: new HashSet<Type> { typeof(Product) });

        await CreateConfigurator(names).EnsureConfiguredAsync(model, CancellationToken.None);

        var columns = await ReadPublicationColumnsAsync(names.Publication);
        columns["products"].ShouldBeNull();
        columns["categories"].ShouldNotBeNull();
    }

    [Test]
    public async Task Manage_publication_tables_false_leaves_the_publication_untouched()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var model = BuildTestModel();

        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await PgExec.ExecuteAsync(
            conn, $"CREATE PUBLICATION {PgExec.QuoteIdentifier(names.Publication)} FOR TABLE public.products", default);

        await CreateConfigurator(names, manageTables: false).EnsureConfiguredAsync(model, CancellationToken.None);

        var columns = await ReadPublicationColumnsAsync(names.Publication);
        columns.Count.ShouldBe(1); // membership not reconciled
        columns["products"].ShouldBeNull(); // no list applied
    }

    [Test]
    public async Task For_all_tables_publication_fails_with_guidance()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var model = BuildTestModel();

        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await PgExec.ExecuteAsync(
            conn, $"CREATE PUBLICATION {PgExec.QuoteIdentifier(names.Publication)} FOR ALL TABLES", default);

        var ex = await Should.ThrowAsync<WallabyConfigurationException>(
            async () => await CreateConfigurator(names).EnsureConfiguredAsync(model, CancellationToken.None));

        ex.Message.ShouldContain("FOR ALL TABLES");
        ex.Message.ShouldContain("ManagePublicationTables");
    }

    [Test]
    public async Task Generated_columns_are_omitted_from_the_list()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);

        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await PgExec.ExecuteAsync(
            conn,
            """
            CREATE TABLE IF NOT EXISTS public.gen_test (
                id int PRIMARY KEY,
                name text NOT NULL,
                upper_name text GENERATED ALWAYS AS (upper(name)) STORED)
            """,
            default);

        // A hand-built one-table model; the capture side never materializes gen_test, this only
        // exercises the attgenerated detection in column-list resolution.
        var genTable = new CapturedTable
        {
            EntityClrType = typeof(object),
            Schema = "public",
            TableName = "gen_test",
            Columns =
            [
                new CapturedColumn { PropertyName = "Id", ColumnName = "id", ClrType = typeof(int), IsPrimaryKey = true },
                new CapturedColumn { PropertyName = "Name", ColumnName = "name", ClrType = typeof(string), IsPrimaryKey = false },
                new CapturedColumn { PropertyName = "UpperName", ColumnName = "upper_name", ClrType = typeof(string), IsPrimaryKey = false },
            ],
            PrimaryKey = [],
        };
        var model = new WallabyModel([genTable], []);

        try
        {
            await CreateConfigurator(names).EnsureConfiguredAsync(model, CancellationToken.None);

            var columns = await ReadPublicationColumnsAsync(names.Publication);
            columns["gen_test"]!.ShouldBe(["id", "name"], ignoreOrder: true);
        }
        finally
        {
            await PgExec.ExecuteAsync(conn, "DROP TABLE IF EXISTS public.gen_test", default);
        }
    }

    [Test]
    public async Task Scoped_destination_mapping_publishes_whole_rows()
    {
        // Scoped destinations require full old-row values on deletes; the harness (like production
        // ToCaptureSpec) must flag the table so it is never column-listed.
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null,
            p => new WallabyDocument { ["name"] = p.Name },
            scopeKey: p => p.TenantId,
            scopedDestination: key => $"t_{key}");
        await harness.SelfConfigureAsync();

        (await ReadPublicationColumnsAsync(harness.Names.Publication))["products"].ShouldBeNull();
    }

    [Test]
    public async Task Excluded_toasted_column_never_arrives_and_does_not_poison_updates()
    {
        // The full stack: ConsumesAllExcept -> publication column list -> stream -> materialize -> sink.
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString)
            .ConsumesAllExcept<Product>(nameof(Product.Description))
            .Broadcast().Capture<Product>();
        var capture = harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        // The column is excluded at the wire, not just at materialization.
        (await ReadPublicationColumnsAsync(harness.Names.Publication))["products"]!
            .ShouldNotContain(nameof(Product.Description));

        var categoryId = await harness.Db.AddCategoryAsync();
        var productId = await harness.Db.AddProductAsync(categoryId, "toasty");
        // Incompressible ~128KB payload so the value is TOASTed out-of-line.
        var payload = string.Concat(Enumerable.Range(0, 4096).Select(_ => Guid.NewGuid().ToString("N")));
        await harness.Db.SetProductDescriptionAsync(productId, payload);
        // REPLICA IDENTITY DEFAULT + an unchanged TOASTed column: without the exclusion this UPDATE
        // would be a poison change.
        await harness.Db.UpdateProductNameAsync(productId, "renamed");

        await harness.RunUntilAsync(() => capture.For("products")
            .Any(r => r.Document?.GetValueOrDefault("Name") as string == "renamed"));

        var renamed = capture.For("products")
            .First(r => r.Document?.GetValueOrDefault("Name") as string == "renamed");
        renamed.Document!.ContainsKey(nameof(Product.Description)).ShouldBeFalse();
        capture.For("products").ShouldAllBe(r =>
            r.Document == null || !r.Document.ContainsKey(nameof(Product.Description)));
    }

    [Test]
    public async Task Consumes_selection_limits_the_delivered_record()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString)
            .Consumes<Product>(nameof(Product.Name))
            .Broadcast().Capture<Product>();
        var capture = harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "only-name");

        await harness.RunUntilAsync(() => capture.For("products")
            .Any(r => r.Document?.GetValueOrDefault("Name") as string == "only-name"));

        var record = capture.For("products")
            .First(r => r.Document?.GetValueOrDefault("Name") as string == "only-name").Document!;
        record.ContainsKey(nameof(Product.Id)).ShouldBeTrue();
        record.ContainsKey(nameof(Product.Price)).ShouldBeFalse();
        record.ContainsKey(nameof(Product.CategoryId)).ShouldBeFalse();
    }

    [Test]
    public async Task Mid_stream_column_list_change_decodes_subsequent_changes()
    {
        // A publication change re-sends the RelationMessage; the assembler must refresh its cached
        // per-relation read plan rather than decode the narrower tuple with the stale layout.
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var capture = harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.StartAsync();
        try
        {
            await harness.Db.AddProductAsync(categoryId, "before");
            await harness.WaitUntilAsync(() => capture.For("products")
                .Any(r => r.Document?.GetValueOrDefault("Name") as string == "before"));

            await using (var conn = new NpgsqlConnection(pg.ConnectionString))
            {
                await conn.OpenAsync();
                await PgExec.ExecuteAsync(
                    conn,
                    $"""
                     ALTER PUBLICATION {PgExec.QuoteIdentifier(harness.Names.Publication)} SET TABLE
                     public.products ("Id", "TenantId", "Name", "Price", "product_sku", "Status", "Tags", "CategoryId")
                     """,
                    default);
            }

            await harness.Db.AddProductAsync(categoryId, "after");
            await harness.WaitUntilAsync(() => capture.For("products")
                .Any(r => r.Document?.GetValueOrDefault("Name") as string == "after"));

            var after = capture.For("products")
                .First(r => r.Document?.GetValueOrDefault("Name") as string == "after");
            after.Document!.ContainsKey(nameof(Product.Description)).ShouldBeFalse();
        }
        finally
        {
            await harness.StopAsync();
        }
    }

    [Test]
    public async Task Postgres_14_fails_fast_with_version_guidance()
    {
        await using var pg14 = new PostgreSqlBuilder("postgres:14").Build();
        await pg14.StartAsync();
        try
        {
            await using var source = NpgsqlDataSource.Create(pg14.GetConnectionString());
            var configurator = new PostgresSelfConfigurator(
                source, new SelfConfigOptions { SlotName = "x", PublicationName = "y" }, NullLogger.Instance);

            var ex = await Should.ThrowAsync<WallabyConfigurationException>(
                async () => await configurator.EnsureConfiguredAsync(BuildTestModel(), CancellationToken.None));

            ex.Message.ShouldContain("PostgreSQL 15");
        }
        finally
        {
            await pg14.DisposeAsync();
        }
    }
}
