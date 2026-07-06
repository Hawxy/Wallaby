using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;

namespace Sample.WorkerApp;

/// <summary>Projects each product into a flat Meilisearch document. Resolved from the container.</summary>
public sealed class ProductSearchTransform(ILogger<ProductSearchTransform> logger) : IWallabyEfTransform<Product>
{
    public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        DbContext db, IReadOnlyList<ChangeEvent<Product>> changes, CancellationToken ct)
    {
        logger.LogDebug("Transforming {Count} product change(s)", changes.Count);

        var documents = new Dictionary<DocumentKey, WallabyDocument?>(changes.Count);
        foreach (var change in changes)
        {
            var product = change.Entity!;
            documents[change.Key] = new WallabyDocument
            {
                ["name"] = product.Name,
                ["price"] = product.Price,
                ["category"] = product.Category,
            };
        }
        return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
    }
}

/// <summary>Routes product changes to the "products" index; bump the version to force a reindex.</summary>
public sealed class ProductSearchMapping : IWallabyEntityMapping<Product>
{
    public void Configure(EntityMapBuilder<Product> map) => map
        .ToDestination("products")
        .WithBackfillVersion("v1")
        .UsingTransform<Product, ProductSearchTransform>();
}
