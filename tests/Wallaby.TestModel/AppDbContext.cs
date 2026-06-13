using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace Wallaby.TestModel;

/// <summary>
/// Representative EF Core model used across unit and integration tests: single and composite primary
/// keys, a custom column name, a value-converted enum, a jsonb column, a TOAST-prone text column, and
/// a non-default schema.
/// </summary>
public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Category> Categories => Set<Category>();
    public DbSet<Product> Products => Set<Product>();
    public DbSet<Label> Labels => Set<Label>();
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OrderLine> OrderLines => Set<OrderLine>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Category>(b =>
        {
            b.ToTable("categories");
            b.HasKey(c => c.Id);
        });

        modelBuilder.Entity<Product>(b =>
        {
            b.ToTable("products");
            b.HasKey(p => p.Id);
            b.Property(p => p.Sku).HasColumnName("product_sku");
            b.Property(p => p.Status).HasConversion<string>().HasMaxLength(32);
            b.Property(p => p.Tags)
                .HasColumnType("jsonb")
                .HasConversion(
                    v => JsonSerializer.Serialize(v, (JsonSerializerOptions?)null),
                    v => JsonSerializer.Deserialize<List<string>>(v, (JsonSerializerOptions?)null) ?? new List<string>());
            b.Property(p => p.Description).HasColumnType("text");
            b.HasOne(p => p.Category).WithMany(c => c.Products).HasForeignKey(p => p.CategoryId);
            b.HasMany(p => p.Labels)
                .WithMany(l => l.Products)
                .UsingEntity(j => j.ToTable("product_labels"));
        });

        modelBuilder.Entity<Label>(b =>
        {
            b.ToTable("labels");
            b.HasKey(l => l.Id);
        });

        modelBuilder.Entity<Customer>(b =>
        {
            b.ToTable("customers");
            b.HasKey(c => c.Id);
        });

        modelBuilder.Entity<Order>(b =>
        {
            b.ToTable("orders", schema: "sales");
            b.HasKey(o => o.Id);
            b.HasOne(o => o.Customer).WithMany(c => c.Orders).HasForeignKey(o => o.CustomerId);
        });

        modelBuilder.Entity<OrderLine>(b =>
        {
            b.ToTable("order_lines", schema: "sales");
            b.HasKey(l => new { l.OrderId, l.LineNumber });
        });
    }
}
