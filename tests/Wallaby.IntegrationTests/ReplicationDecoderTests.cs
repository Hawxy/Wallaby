using EFCore.CDC.TestModel;
using Microsoft.Extensions.Logging.Abstractions;
using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Model;
using Wallaby.TestInfrastructure;

namespace Wallaby.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class ReplicationDecoderTests(PostgresFixture pg)
{
    private static CdcModel BuildAllMappedModel()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        return ModelToCdcModel.Build(ctx.Model, new CaptureSpec { CaptureAllMapped = true });
    }

    [Test]
    public async Task Insert_update_delete_produce_correct_raw_changes()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        var pub = $"cdc_pub_{suffix}";

        // 1) Create slot + publication (captures from now on).
        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions { SlotName = slot, PublicationName = pub },
            NullLogger.Instance);
        await configurator.EnsureConfiguredAsync(BuildAllMappedModel(), CancellationToken.None);

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
        await using var spill = new PostgresUnloggedTableSpill(pg.DataSource, slot);
        await using var stream = new LogicalReplicationStream(pg.ConnectionString, slot, pub, spill);
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
        await Assert.That(insert.Schema).IsEqualTo("public");
        await Assert.That(insert.TableName).IsEqualTo("products");
        await Assert.That(insert.OldValues).IsNull();
        await Assert.That(insert.CommitLsn).IsGreaterThan(0UL);
        await Assert.That(Convert.ToInt32(Value(insert.NewValues, "Id"))).IsEqualTo(productId);
        await Assert.That(Value(insert.NewValues, "Name")).IsEqualTo("Widget");
        await Assert.That(Value(insert.NewValues, "product_sku")).IsEqualTo("W-1");

        // Update (REPLICA IDENTITY DEFAULT + non-key change => new values only, no old tuple)
        var update = collected.Single(c => c.Action == ChangeAction.Update);
        await Assert.That(Value(update.NewValues, "Name")).IsEqualTo("Widget v2");
        await Assert.That(update.OldValues).IsNull();

        // Delete (old values carry the primary key under REPLICA IDENTITY DEFAULT)
        var delete = collected.Single(c => c.Action == ChangeAction.Delete);
        await Assert.That(delete.NewValues.Count).IsEqualTo(0);
        await Assert.That(Convert.ToInt32(Value(delete.OldValues!, "Id"))).IsEqualTo(productId);
    }

    private static object? Value(IReadOnlyList<RawColumn> columns, string name)
        => columns.Single(c => c.ColumnName == name).Value;
}
