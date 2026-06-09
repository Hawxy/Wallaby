using EFCore.CDC.TestModel;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.UnitTests;

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
        var meta = new ChangeMetadata("public", "products", DateTimeOffset.UtcNow, 1, 0, IsBackfill: false);
        return new ChangeEvent(ChangeAction.Insert, meta, new Product { Id = id, Name = "x" },
            record, Changes: null, new object[] { id }) { EntityClrType = typeof(Product) };
    }

    [Test]
    public async Task ScopedBy_change_overload_reads_the_scope_key_from_the_record()
    {
        var builder = new CdcBuilder();
        builder.UseContext<AppDbContext>();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success));
        // ScopedBy is only valid when something consumes the key — here a scoped enrichment context.
        builder.UseScopedContext((_, _) =>
            new AppDbContext(TestModelFactory.CreateOptions("Host=localhost;Database=db;Username=u;Password=p")));
        builder.Map<Product>()
            .ToSink("sink", "products")
            .UsingTransform((_, _, _) =>
                Task.FromResult<IReadOnlyDictionary<DocumentKey, CdcDocument?>>(new Dictionary<DocumentKey, CdcDocument?>()))
            .ScopedBy(c => c.Record.GetValueOrDefault("TenantId"));

        var config = builder.Build();
        var selector = config.Mappings[typeof(Product)].ScopeKeySelector;

        await Assert.That(selector).IsNotNull();
        var key = selector!(Insert(1, new Dictionary<string, object?> { ["TenantId"] = "tenant-a" }));
        await Assert.That(key).IsEqualTo("tenant-a");
    }
}
