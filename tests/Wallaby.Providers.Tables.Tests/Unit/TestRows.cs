using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Wallaby.Providers.Tables.Tests.Unit;

public enum Status
{
    Pending,
    Paid,
}

/// <summary>Positional record with attribute naming: the primary constructor is the only one.</summary>
[Table("orders", Schema = "sales")]
public sealed record Order(int Id, [property: Column("customer_ref")] string CustomerRef, decimal Total, Status Status, DateTimeOffset? PaidAt);

/// <summary>Unannotated class: conventions decide the table, columns and key.</summary>
public sealed class Widget
{
    public Guid Id { get; set; }
    public string DisplayName { get; set; } = "";
    public int? Qty { get; set; }
    public int[] Tags { get; set; } = [];
    public string Derived => DisplayName.ToUpperInvariant();
    [NotMapped] public string Scratch { get; set; } = "";
}

/// <summary>Composite key declared fluently.</summary>
public sealed class OrderLine
{
    public int OrderId { get; set; }
    public int LineNo { get; set; }
    public string Sku { get; set; } = "";
}

/// <summary>Key attributes with an explicit column order.</summary>
public sealed class KeyedByAttribute
{
    [Key, Column(Order = 1)] public string Region { get; set; } = "";
    [Key, Column(Order = 0)] public int Code { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>Type-name key convention (<c>InvoiceId</c>).</summary>
public sealed class Invoice
{
    public long InvoiceId { get; set; }
    public string Number { get; set; } = "";
}

public sealed class NoKey
{
    public string Name { get; set; } = "";
}

public sealed class NestedProperty
{
    public int Id { get; set; }
    public Widget Child { get; set; } = new();
}

public sealed class ListProperty
{
    public int Id { get; set; }
    public List<string> Items { get; set; } = [];
}

public sealed class UnsupportedKey
{
    public int[] Id { get; set; } = [];
}

/// <summary>Two constructors, neither parameterless: ambiguous.</summary>
public sealed class TwoCtors
{
    public TwoCtors(int id) => Id = id;
    public TwoCtors(int id, string name) { Id = id; Name = name; }
    public int Id { get; }
    public string Name { get; } = "";
}

/// <summary>A constructor parameter with no matching property.</summary>
public sealed class UnmatchedCtor
{
    public UnmatchedCtor(int id, string label) { Id = id; Name = label; }
    public int Id { get; }
    public string Name { get; }
}

public sealed class Unregistered
{
    public int Id { get; set; }
}
