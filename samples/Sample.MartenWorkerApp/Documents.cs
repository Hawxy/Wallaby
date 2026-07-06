namespace Sample.MartenWorkerApp;

/// <summary>An order document kept in sync with a Meilisearch "orders" index.</summary>
public class Order
{
    public Guid Id { get; set; }
    public string Number { get; set; } = "";
    public Guid CustomerId { get; set; }
    public decimal Total { get; set; }
}

/// <summary>Enrichment-only document: not mapped to a sink, queried by the order transform.</summary>
public class Customer
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}
