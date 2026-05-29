using EFCore.CDC.TestModel;

namespace EFCore.CDC.Testing;

/// <summary>Convenience seed/mutation helpers for the <see cref="AppDbContext"/> test model, bound to a connection.</summary>
public sealed class TestDatabase(string connectionString)
{
    private AppDbContext NewContext() => new(TestModelFactory.CreateOptions(connectionString));

    public async Task<int> AddCategoryAsync(string name = "Cat")
    {
        await using var ctx = NewContext();
        var category = new Category { Name = name };
        ctx.Categories.Add(category);
        await ctx.SaveChangesAsync();
        return category.Id;
    }

    public async Task<int> AddProductAsync(int categoryId, string name, decimal price = 1m)
    {
        await using var ctx = NewContext();
        var product = new Product
        {
            Name = name, Price = price, Sku = name, Status = ProductStatus.Active,
            Tags = [], Description = "", CategoryId = categoryId,
        };
        ctx.Products.Add(product);
        await ctx.SaveChangesAsync();
        return product.Id;
    }

    public async Task<IReadOnlyList<(int Id, string Name)>> AddProductsAsync(int categoryId, params string[] names)
    {
        await using var ctx = NewContext();
        var products = names.Select(n => new Product
        {
            Name = n, Price = 1m, Sku = n, Status = ProductStatus.Active, Tags = [], Description = "", CategoryId = categoryId,
        }).ToList();
        ctx.Products.AddRange(products);
        await ctx.SaveChangesAsync();
        return products.Select(p => (p.Id, p.Name)).ToList();
    }

    public async Task UpdateProductNameAsync(int id, string name)
    {
        await using var ctx = NewContext();
        var product = await ctx.Products.FindAsync(id);
        product!.Name = name;
        await ctx.SaveChangesAsync();
    }

    public async Task DeleteProductAsync(int id)
    {
        await using var ctx = NewContext();
        ctx.Products.Remove((await ctx.Products.FindAsync(id))!);
        await ctx.SaveChangesAsync();
    }

    /// <summary>Create a customer + order with <paramref name="lineCount"/> lines (single transaction for the order).</summary>
    public async Task<int> AddOrderWithLinesAsync(string customerName, int lineCount)
    {
        await using var ctx = NewContext();
        var customer = new Customer { Name = customerName, Email = $"{customerName}@example.com" };
        ctx.Customers.Add(customer);
        await ctx.SaveChangesAsync();

        var order = new Order { CustomerId = customer.Id, CreatedAt = DateTimeOffset.UtcNow };
        for (var i = 1; i <= lineCount; i++)
        {
            order.Lines.Add(new OrderLine { LineNumber = i, ProductId = i, Quantity = 1, UnitPrice = 1m });
        }
        ctx.Orders.Add(order);
        await ctx.SaveChangesAsync();
        return order.Id;
    }
}
