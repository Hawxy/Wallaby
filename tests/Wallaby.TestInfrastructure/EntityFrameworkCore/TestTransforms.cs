using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.TestModel;

namespace Wallaby.TestInfrastructure.EntityFrameworkCore;

/// <summary>Stock transforms for production-style <c>WithMappings</c> registrations in end-to-end tests.</summary>
public static class TestTransforms
{
    /// <summary>
    /// Projects each <see cref="Product"/> change to <c>{ "name": product.Name }</c>. Pass as a method
    /// group to the EF Core <c>UsingTransform</c> overload.
    /// </summary>
    public static Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> ProductNames(
        DbContext db, IReadOnlyList<ChangeEvent<Product>> changes, CancellationToken ct)
    {
        var docs = new Dictionary<DocumentKey, WallabyDocument?>();
        foreach (var c in changes)
        {
            docs[c.Key] = new WallabyDocument { ["name"] = c.Entity!.Name };
        }
        return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(docs);
    }
}
