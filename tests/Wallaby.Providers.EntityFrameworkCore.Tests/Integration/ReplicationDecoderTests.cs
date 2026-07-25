using Microsoft.Extensions.Logging.Abstractions;
using Wallaby.Abstractions;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Model;
using Wallaby.Providers;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class ReplicationDecoderTests(TestModelPostgresFixture pg)
{
    private static WallabyModel BuildTestModel()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        return EfCoreCaptureModelBuilder.Build(
            ctx.Model, new CaptureSpec { DeclaredEntities = [typeof(Category), typeof(Product)] });
    }

    [Test]
    public async Task Insert_update_delete_produce_correct_raw_changes()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);

        // 1) Create slot + publication (captures from now on).
        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions { SlotName = names.Slot, PublicationName = names.Publication },
            NullLogger.Instance);
        await configurator.EnsureConfiguredAsync(BuildTestModel(), CancellationToken.None);

        // 2) Generate changes on the products table (each SaveChanges is its own transaction).
        int productId;
        await using (var ctx = new AppDbContext(TestModelFactory.CreateOptions(pg.ConnectionString)))
        {
            var category = new Category { Name = "Books" };
            ctx.Categories.Add(category);
            await ctx.SaveChangesAsync();

            var product = new Product
            {
                Name = "Widget",
                Price = 9.99m,
                Sku = "W-1",
                Status = ProductStatus.Active,
                Tags = ["a", "b"],
                Description = "a widget",
                CategoryId = category.Id,
            };
            ctx.Products.Add(product);
            await ctx.SaveChangesAsync();
            productId = product.Id;

            product.Name = "Widget v2";
            await ctx.SaveChangesAsync();

            ctx.Products.Remove(product);
            await ctx.SaveChangesAsync();
        }

        // 3) Stream and collect the product changes.
        var collected = new List<RawChange>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await using var spill = new PostgresUnloggedTableSpill(pg.DataSource, names.Slot);
        await using var stream = new LogicalReplicationStream(pg.ConnectionString, names.Slot, names.Publication, spill);
        try
        {
            await foreach (var txn in stream.ReadAsync(cts.Token))
            {
                collected.AddRange(txn.Changes.Where(c => c.TableName == "products"));
                if (collected.Any(c => c.Action == ChangeAction.Delete))
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Fall through to assertions, which will report what was (not) captured.
        }

        // Insert
        var insert = collected.Single(c => c.Action == ChangeAction.Insert);
        insert.Schema.ShouldBe("public");
        insert.TableName.ShouldBe("products");
        insert.OldValues.ShouldBeNull();
        insert.CommitLsn.ShouldBeGreaterThan(0UL);
        Convert.ToInt32(Value(insert.NewValues, "Id")).ShouldBe(productId);
        Value(insert.NewValues, "Name").ShouldBe("Widget");
        Value(insert.NewValues, "product_sku").ShouldBe("W-1");
        // jsonb with the default ColumnReadMode keeps its string representation (Product.Tags's
        // ValueConverter expects a string).
        Value(insert.NewValues, "Tags").ShouldBeOfType<string>();

        // Update (REPLICA IDENTITY DEFAULT + non-key change => new values only, no old tuple)
        var update = collected.Single(c => c.Action == ChangeAction.Update);
        Value(update.NewValues, "Name").ShouldBe("Widget v2");
        update.OldValues.ShouldBeNull();

        // Delete (old values carry the primary key under REPLICA IDENTITY DEFAULT)
        var delete = collected.Single(c => c.Action == ChangeAction.Delete);
        delete.NewValues.Count.ShouldBe(0);
        Convert.ToInt32(Value(delete.OldValues!, "Id")).ShouldBe(productId);
    }

    [Test]
    public async Task Truncate_of_captured_tables_is_surfaced_and_unmapped_tables_are_ignored()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);

        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions { SlotName = names.Slot, PublicationName = names.Publication },
            NullLogger.Instance);
        await configurator.EnsureConfiguredAsync(BuildTestModel(), CancellationToken.None);

        // A published-but-unmapped table (the user-managed-publication scenario).
        await ExecAsync("CREATE TABLE truncate_noise (id int PRIMARY KEY)");
        try
        {
            await ExecAsync($"ALTER PUBLICATION {names.Publication} ADD TABLE truncate_noise");

            // CASCADE pulls in unpublished FK children (product_labels), which never reach the stream.
            await ExecAsync("TRUNCATE TABLE products, truncate_noise CASCADE");
            await ExecAsync("TRUNCATE TABLE truncate_noise");
            await ExecAsync("TRUNCATE TABLE categories CASCADE");

            // Terminator so the stream loop knows all truncates are in.
            await using (var ctx = new AppDbContext(TestModelFactory.CreateOptions(pg.ConnectionString)))
            {
                ctx.Categories.Add(new Category { Name = "terminator" });
                await ctx.SaveChangesAsync();
            }

            var collected = new List<(IReadOnlyList<string> Tables, int Changes)>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await using var spill = new PostgresUnloggedTableSpill(pg.DataSource, names.Slot);
            await using var stream = new LogicalReplicationStream(
                pg.ConnectionString, names.Slot, names.Publication, spill, model: BuildTestModel());
            await foreach (var txn in stream.ReadAsync(cts.Token))
            {
                if (txn.Changes.Any(c => c.TableName == "categories" && c.Action == ChangeAction.Insert))
                {
                    break;
                }
                collected.Add((txn.TruncatedTables, txn.Changes.Count));
            }

            collected.Count.ShouldBe(3);

            // Mixed truncate: the unmapped noise table is filtered out; no changes are synthesized.
            collected[0].Tables.ShouldBe(new[] { "public.products" });
            collected[0].Changes.ShouldBe(0);

            // Unmapped-only truncate: delivered as an empty transaction, nothing surfaced.
            collected[1].Tables.ShouldBeEmpty();

            // CASCADE reaches the captured products table through the FK.
            collected[2].Tables.ShouldBe(new[] { "public.categories", "public.products" }, ignoreOrder: true);
        }
        finally
        {
            await ExecAsync("DROP TABLE IF EXISTS truncate_noise");
        }
    }

    [Test]
    public async Task A_null_array_element_decodes_per_instance_and_round_trips_through_spill()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var tableName = names.Named("array_probe");
        await ExecAsync($"CREATE TABLE {tableName} (id int PRIMARY KEY, nums int[])");
        try
        {
            var configurator = new PostgresSelfConfigurator(
                pg.DataSource,
                new SelfConfigOptions { SlotName = names.Slot, PublicationName = names.Publication },
                NullLogger.Instance);
            await configurator.EnsureConfiguredAsync(new WallabyModel([ProbeTable(tableName)]), CancellationToken.None);

            await ExecAsync($"INSERT INTO {tableName} (id, nums) VALUES (1, ARRAY[1, NULL, 3]), (2, ARRAY[4, 5])");

            var collected = await CollectAsync(names, c => c.TableName == tableName, count: 2);

            // PerInstance decoding: the element type follows the row's contents.
            var withNull = collected.Single(c => Convert.ToInt32(Value(c.NewValues, "id")) == 1);
            Value(withNull.NewValues, "nums").ShouldBeOfType<int?[]>().ShouldBe(new int?[] { 1, null, 3 });
            var withoutNull = collected.Single(c => Convert.ToInt32(Value(c.NewValues, "id")) == 2);
            Value(withoutNull.NewValues, "nums").ShouldBeOfType<int[]>().ShouldBe([4, 5]);

            // The spill preserves the null element and the CLR array type.
            var spilled = SpillCodec.Decode(SpillCodec.Encode(withNull));
            Value(spilled.NewValues, "nums").ShouldBeOfType<int?[]>().ShouldBe(new int?[] { 1, null, 3 });
        }
        finally
        {
            await ExecAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Test]
    public async Task A_decode_failure_names_the_column_and_table()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var tableName = names.Named("overflow_probe");
        await ExecAsync($"CREATE TABLE {tableName} (id int PRIMARY KEY, amount numeric)");
        try
        {
            var configurator = new PostgresSelfConfigurator(
                pg.DataSource,
                new SelfConfigOptions { SlotName = names.Slot, PublicationName = names.Publication },
                NullLogger.Instance);
            await configurator.EnsureConfiguredAsync(new WallabyModel([ProbeTable(tableName)]), CancellationToken.None);

            // numeric holds values no System.Decimal can represent; the read throws at decode time.
            await ExecAsync($"INSERT INTO {tableName} (id, amount) VALUES (1, '1e40'::numeric)");

            var ex = await Should.ThrowAsync<InvalidOperationException>(
                () => CollectAsync(names, c => c.TableName == tableName, count: 1));

            ex.Message.ShouldContain("amount");
            ex.Message.ShouldContain(tableName);
            ex.Message.ShouldContain("WAL position");
            ex.InnerException.ShouldNotBeNull();
        }
        finally
        {
            await ExecAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private static CapturedTable ProbeTable(string tableName) => new()
    {
        EntityClrType = typeof(object),
        Schema = "public",
        TableName = tableName,
        Columns = [],
        PrimaryKey = [],
    };

    private async Task<List<RawChange>> CollectAsync(ReplicationScope names, Func<RawChange, bool> filter, int count)
    {
        var collected = new List<RawChange>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await using var spill = new PostgresUnloggedTableSpill(pg.DataSource, names.Slot);
        await using var stream = new LogicalReplicationStream(pg.ConnectionString, names.Slot, names.Publication, spill);
        await foreach (var txn in stream.ReadAsync(cts.Token))
        {
            collected.AddRange(txn.Changes.Where(filter));
            if (collected.Count >= count)
            {
                break;
            }
        }
        return collected;
    }

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    private static object? Value(IReadOnlyList<RawColumn> columns, string name)
        => columns.Single(c => c.ColumnName == name).Value;
}
