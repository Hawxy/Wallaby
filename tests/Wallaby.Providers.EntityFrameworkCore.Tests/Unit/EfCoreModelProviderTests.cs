using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit;

/// <summary>
/// The EF Core provider's external-slot table resolution: every physical table once, and owned types stored
/// in their owner's table rejected by name.
/// </summary>
public class EfCoreModelProviderTests
{
    [Test]
    public async Task ResolveAllTables_lists_every_physical_table_once()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var tables = new EfCoreModelProvider(ctx.Model).ResolveAllTables()
            .Select(t => $"{t.Schema}.{t.Table}").ToList();

        // Same-table and JSON owned types collapse onto suppliers; separate-table owned types and the
        // many-to-many join table appear on their own.
        tables.ShouldBe(
        [
            "public.categories", "public.products", "public.labels", "public.product_labels", "public.customers",
            "public.suppliers", "public.supplier_notes", "public.supplier_legal", "sales.orders", "sales.order_lines",
        ], ignoreOrder: true);
    }

    [Test]
    public async Task ResolveTable_rejects_an_owned_type_stored_in_its_owners_table()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var provider = new EfCoreModelProvider(ctx.Model);

        Should.Throw<WallabyConfigurationException>(() => provider.ResolveTable(typeof(SupplierMeta)))
            .Message.ShouldContain("public.suppliers");
        Should.Throw<WallabyConfigurationException>(() => provider.ResolveTable(typeof(Address)));

        // An owned type with its own table resolves to it.
        provider.ResolveTable(typeof(SupplierNote)).Table.ShouldBe("supplier_notes");
    }
}
