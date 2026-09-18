using System.ComponentModel.DataAnnotations.Schema;

namespace Wallaby.TestInfrastructure.Tables;

public enum OrderStatus
{
    Pending,
    Paid,
    Shipped,
}

/// <summary>Positional record over <c>tables.orders</c>; <see cref="Notes"/> is the TOAST-prone column.</summary>
[Table("orders", Schema = TablesFixture.Schema)]
public sealed record Order(int Id, string CustomerRef, decimal Total, OrderStatus Status, DateTimeOffset CreatedAt, string? Notes);

/// <summary>Composite key (<c>order_id</c>, <c>line_no</c>) declared fluently by the fixture.</summary>
[Table("order_lines", Schema = TablesFixture.Schema)]
public sealed class OrderLine
{
    public int OrderId { get; set; }
    public int LineNo { get; set; }
    public string Sku { get; set; } = "";
    public int Qty { get; set; }
}

/// <summary>Unannotated class: snake_case convention maps it to <c>tables.gizmo</c> (REPLICA IDENTITY FULL).</summary>
public sealed class Gizmo
{
    public Guid Id { get; set; }
    public string DisplayName { get; set; } = "";
    public decimal UnitPrice { get; set; }
}
