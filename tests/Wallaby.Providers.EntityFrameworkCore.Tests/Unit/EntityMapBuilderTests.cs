using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit;

/// <summary>
/// The <see cref="ChangeEvent"/>-based <c>ScopedBy</c> overload lets the scope key come from a captured column
/// that is NOT a CLR property of the entity — e.g. a Finbuckle multi-tenancy shadow <c>tenant_id</c> read via
/// <c>change.Record["TenantId"]</c>. It must store the selector directly (no per-change <see cref="ChangeEvent"/>
/// allocation) and read it from the raw change.
/// </summary>
public class EntityMapBuilderTests
{
    private static ChangeEvent Insert(int id, IReadOnlyDictionary<string, object?> record)
    {
        var meta = new ChangeMetadata("public", "products", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, IsBackfill: false);
        return new ChangeEvent(ChangeAction.Insert, meta, new Product { Id = id, Name = "x" },
            record, Changes: null, new object[] { id }) { EntityClrType = typeof(Product) };
    }

    [Test]
    public void ScopedBy_change_overload_reads_the_scope_key_from_the_record()
    {
        var builder = new WallabyBuilder();
        builder.UseEntityFrameworkCore<AppDbContext>();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        var sink = builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success));
        // ScopedBy is only valid when something consumes the key — here a scoped enrichment context.
        builder.UseScopedDbContext((_, _) =>
            new AppDbContext(TestModelFactory.CreateOptions("Host=localhost;Database=db;Username=u;Password=p")));
        sink.WithMappings(s => s.Map<Product>()
            .ToDestination("products")
            .UsingTransform((_, _, _) =>
                Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(new Dictionary<DocumentKey, WallabyDocument?>()))
            .ScopedBy(c => c.Record.GetValueOrDefault("TenantId")));

        var config = builder.Build();
        var selector = config.AllMappings.Single().ScopeKeySelector;

        selector.ShouldNotBeNull();
        var key = selector!(Insert(1, new Dictionary<string, object?> { ["TenantId"] = "tenant-a" }));
        key.ShouldBe("tenant-a");
    }
}
