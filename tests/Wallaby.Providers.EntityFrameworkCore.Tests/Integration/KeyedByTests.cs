using Wallaby.Abstractions;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class KeyedByTests(TestModelPostgresFixture pg)
{
    [Test]
    public async Task Delete_of_a_keyed_by_row_removes_the_custom_keyed_document()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Map<Product>("capture", destination: null, (_, changes, _) =>
                Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(changes.ToDictionary(
                    c => c.Key,
                    c => (WallabyDocument?)new WallabyDocument { ["name"] = c.Entity?.Name })),
            keyedBy: p => p.Sku);

        // KeyedBy needs the full old row on delete to compute the custom id.
        await harness.Db.SetReplicaIdentityFullAsync("products");
        try
        {
            await harness.SelfConfigureAsync();
            await harness.StartAsync();

            var categoryId = await harness.Db.AddCategoryAsync();
            var productId = await harness.Db.AddProductAsync(categoryId, "keyed_sku"); // Sku = name
            await harness.WaitUntilAsync(() => capture.For("products").Any(r => !r.IsDeletion));

            await harness.Db.DeleteProductAsync(productId);
            await harness.WaitUntilAsync(() => capture.For("products").Any(r => r.IsDeletion));

            // Both sides of the document's lifecycle used the custom key, so the delete removes
            // the document the insert created — not a PK-named one that never existed.
            capture.For("products").Single(r => !r.IsDeletion).DocumentId.ShouldBe("keyed_sku");
            capture.For("products").Single(r => r.IsDeletion).DocumentId.ShouldBe("keyed_sku");
        }
        finally
        {
            // The session database is shared; later tests expect the default identity.
            await harness.Db.SetReplicaIdentityDefaultAsync("products");
        }
    }
}
