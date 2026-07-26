namespace Wallaby.TestModel;

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
    public int TenantId { get; set; }
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public string Sku { get; set; } = "";
    public ProductStatus Status { get; set; }
    public List<string> Tags { get; set; } = [];
    public string Description { get; set; } = "";
    public int CategoryId { get; set; }
    public Category? Category { get; set; }

    /// <summary>Many-to-many skip-navigation, backed by an implicit shared-type join table.</summary>
    public List<Label> Labels { get; set; } = [];
}

/// <summary>Many-to-many counterpart for <see cref="Product"/> via a skip-navigation.</summary>
public class Label
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public List<Product> Products { get; set; } = [];
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

/// <summary>Ctor-bound owned record, nested inside <see cref="Address"/> (optional).</summary>
public record GeoPoint(double Lat, double Lon);

/// <summary>Same-table owned type with settable members and a nested ctor-bound record.</summary>
public class Address
{
    public string Street { get; set; } = "";
    public string City { get; set; } = "";
    public GeoPoint? Location { get; set; }
}

/// <summary>Ctor-bound complex value object on <see cref="Supplier"/>.</summary>
public record ContactCard(string Email, string Phone);

/// <summary>Owned collection element, stored in its own table (<c>supplier_notes</c>).</summary>
public class SupplierNote
{
    public string Text { get; set; } = "";
}

/// <summary>Owned type mapped to its own table (<c>supplier_legal</c>).</summary>
public class LegalInfo
{
    public string RegistrationNumber { get; set; } = "";
}

/// <summary>Owned type mapped to a JSON column (<c>meta</c>).</summary>
public class SupplierMeta
{
    public string Origin { get; set; } = "";
}

/// <summary>
/// Owned/complex-type showcase: required and optional same-table <c>OwnsOne</c> (with a nested
/// ctor-bound record), a complex property, plus the uncapturable shapes (owned collection,
/// separate-table owned type, JSON-mapped owned type).
/// </summary>
public class Supplier
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public Address Address { get; set; } = new();
    public Address? BillingAddress { get; set; }
    public ContactCard Contact { get; set; } = new("", "");
    public List<SupplierNote> Notes { get; set; } = [];
    public LegalInfo? Legal { get; set; }
    public SupplierMeta? Meta { get; set; }
}
