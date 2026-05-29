namespace EFCore.CDC.TestModel;

/// <summary>Value-converted enum (stored as text) used by <see cref="Product"/>.</summary>
public enum ProductStatus
{
    Draft,
    Active,
    Discontinued,
}

/// <summary>Single-PK entity with a navigation, used as a category for products.</summary>
public class Category
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public List<Product> Products { get; set; } = [];
}

/// <summary>
/// Single-PK entity exercising a custom column name (<c>product_sku</c>), a value-converted enum,
/// a jsonb column, and a large TOAST-prone text column.
/// </summary>
public class Product
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public string Sku { get; set; } = "";
    public ProductStatus Status { get; set; }
    public List<string> Tags { get; set; } = [];
    public string Description { get; set; } = "";
    public int CategoryId { get; set; }
    public Category? Category { get; set; }
}

/// <summary>Single-PK customer entity (placed in the default schema).</summary>
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public List<Order> Orders { get; set; } = [];
}

/// <summary>Single-PK order aggregate root, mapped into a non-default schema (<c>sales</c>).</summary>
public class Order
{
    public int Id { get; set; }
    public int CustomerId { get; set; }
    public Customer? Customer { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public List<OrderLine> Lines { get; set; } = [];
}

/// <summary>Composite-PK line item (<c>OrderId</c> + <c>LineNumber</c>) in the <c>sales</c> schema.</summary>
public class OrderLine
{
    public int OrderId { get; set; }
    public int LineNumber { get; set; }
    public int ProductId { get; set; }
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}
