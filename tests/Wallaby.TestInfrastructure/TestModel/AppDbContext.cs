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
    public DbSet<Supplier> Suppliers => Set<Supplier>();

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

        modelBuilder.Entity<Supplier>(b =>
        {
            b.ToTable("suppliers");
            b.HasKey(s => s.Id);
            b.OwnsOne(s => s.Address, a =>
            {
                a.Property(x => x.Street).HasColumnName("address_street");
                a.Property(x => x.City).HasColumnName("address_city");
                a.OwnsOne(x => x.Location, g =>
                {
                    g.Property(p => p.Lat).HasColumnName("address_lat");
                    g.Property(p => p.Lon).HasColumnName("address_lon");
                });
            });
            b.Navigation(s => s.Address).IsRequired();
            b.OwnsOne(s => s.BillingAddress, a =>
            {
                a.Property(x => x.Street).HasColumnName("billing_street");
                a.Property(x => x.City).HasColumnName("billing_city");
                a.OwnsOne(x => x.Location, g =>
                {
                    g.Property(p => p.Lat).HasColumnName("billing_lat");
                    g.Property(p => p.Lon).HasColumnName("billing_lon");
                });
            });
            b.ComplexProperty(s => s.Contact, c =>
            {
                c.Property(p => p.Email).HasColumnName("contact_email");
                c.Property(p => p.Phone).HasColumnName("contact_phone");
            });
            b.OwnsMany(s => s.Notes, n => n.ToTable("supplier_notes"));
            b.OwnsOne(s => s.Legal, l => l.ToTable("supplier_legal"));
            b.OwnsOne(s => s.Meta, m => m.ToJson("meta"));
        });
    }
}
