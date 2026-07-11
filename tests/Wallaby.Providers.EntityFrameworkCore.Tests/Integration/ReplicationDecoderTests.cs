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
        var names = WallabyNames.Unique();

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
        try
        {
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
        }
        finally
        {
            await PostgresReplicationCleanup.DropAsync(pg.ConnectionString, names);
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
        // jsonb without a ReadAsUtf8Json flag keeps its string representation (Product.Tags's
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

    private static object? Value(IReadOnlyList<RawColumn> columns, string name)
        => columns.Single(c => c.ColumnName == name).Value;
}
