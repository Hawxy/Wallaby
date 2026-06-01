using EFCore.CDC.TestModel;
using Microsoft.EntityFrameworkCore;

namespace EFCore.CDC.TestInfrastructure;

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

    public async Task SetCategoryNameAsync(int id, string name)
    {
        await using var ctx = NewContext();
        var category = await ctx.Categories.FindAsync(id);
        category!.Name = name;
        await ctx.SaveChangesAsync();
    }

    /// <summary>Rename several categories in a single transaction (one CDC batch with multiple dependent changes).</summary>
    public async Task SetCategoryNamesAsync(IEnumerable<(int Id, string Name)> renames)
    {
        await using var ctx = NewContext();
        foreach (var (id, name) in renames)
        {
            var category = await ctx.Categories.FindAsync(id);
            category!.Name = name;
        }
        await ctx.SaveChangesAsync();
    }

    /// <summary>Rename a category and one of its products in the same transaction (dependent + primary in one batch).</summary>
    public async Task RenameCategoryAndProductAsync(int categoryId, string categoryName, int productId, string productName)
    {
        await using var ctx = NewContext();
        var category = await ctx.Categories.FindAsync(categoryId);
        category!.Name = categoryName;
        var product = await ctx.Products.FindAsync(productId);
        product!.Name = productName;
        await ctx.SaveChangesAsync();
    }

    public async Task<int> AddLabelAsync(string name)
    {
        await using var ctx = NewContext();
        var label = new Label { Name = name };
        ctx.Labels.Add(label);
        await ctx.SaveChangesAsync();
        return label.Id;
    }

    public async Task SetLabelNameAsync(int id, string name)
    {
        await using var ctx = NewContext();
        var label = await ctx.Labels.FindAsync(id);
        label!.Name = name;
        await ctx.SaveChangesAsync();
    }

    /// <summary>Add a label link to a product via the EF Core skip-navigation (writes to <c>product_labels</c>).</summary>
    public async Task AttachLabelAsync(int productId, int labelId)
    {
        await using var ctx = NewContext();
        var product = await ctx.Products.Include(p => p.Labels).FirstAsync(p => p.Id == productId);
        var label = await ctx.Labels.FindAsync(labelId);
        product.Labels.Add(label!);
        await ctx.SaveChangesAsync();
    }

    /// <summary>Remove a label link from a product (deletes from <c>product_labels</c>).</summary>
    public async Task DetachLabelAsync(int productId, int labelId)
    {
        await using var ctx = NewContext();
        var product = await ctx.Products.Include(p => p.Labels).FirstAsync(p => p.Id == productId);
        product.Labels.RemoveAll(l => l.Id == labelId);
        await ctx.SaveChangesAsync();
    }

    public async Task<int> AddProductAsync(int categoryId, string name, decimal price = 1m)
        => await AddProductAsync(categoryId, name, tenantId: 0, price);

    public async Task<int> AddProductAsync(int categoryId, string name, int tenantId, decimal price = 1m)
    {
        await using var ctx = NewContext();
        var product = new Product
        {
            Name = name, Price = price, Sku = name, Status = ProductStatus.Active,
            Tags = [], Description = "", CategoryId = categoryId, TenantId = tenantId,
        };
        ctx.Products.Add(product);
        await ctx.SaveChangesAsync();
        return product.Id;
    }

    /// <summary>Insert several products in a single transaction (one CDC batch), each with its own tenant.</summary>
    public async Task<IReadOnlyList<int>> AddProductsAsync(int categoryId, IEnumerable<(string Name, int Tenant)> items)
    {
        await using var ctx = NewContext();
        var products = items.Select(i => new Product
        {
            Name = i.Name, TenantId = i.Tenant, Price = 1m, Sku = i.Name,
            Status = ProductStatus.Active, Tags = [], Description = "", CategoryId = categoryId,
        }).ToList();
        ctx.Products.AddRange(products);
        await ctx.SaveChangesAsync();
        return products.Select(p => p.Id).ToList();
    }

    /// <summary>Set a table's replica identity to FULL so old-row values (incl. scope keys) are present on delete.</summary>
    public async Task SetReplicaIdentityFullAsync(string table)
    {
        await using var ctx = NewContext();
        // Trusted, test-only DDL; a table identifier cannot be parameterized.
#pragma warning disable EF1002, EF1003
        await ctx.Database.ExecuteSqlRawAsync("ALTER TABLE " + table + " REPLICA IDENTITY FULL");
#pragma warning restore EF1002, EF1003
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
