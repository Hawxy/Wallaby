using EFCore.CDC.TestModel;
using Wallaby;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Internal.SelfConfig;

namespace EFCore.CDC.UnitTests;

public class ExternalSlotConfigTests
{
    // A capturing builder: a sink + a declared context, so the external-slot validation runs alongside a
    // primary slot/publication (exercises the collision-with-primary checks).
    private static CdcBuilder MinimalBuilder()
    {
        var builder = new CdcBuilder();
        builder.UseContext<AppDbContext>();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success));
        return builder;
    }

    // A provision-only builder: a connection string + external slots, no context and no sink.
    private static CdcBuilder ProvisionOnlyBuilder()
    {
        var builder = new CdcBuilder();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        return builder;
    }

    [Test]
    public async Task Provision_only_without_a_sink_or_context_is_valid()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("elt", s => s.ForTable("orders"));

        var config = builder.Build();

        await Assert.That(config.CaptureIntended).IsFalse();
        await Assert.That(config.ExternalSlots.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Provision_only_with_no_external_slots_is_valid()
    {
        // The consumer's env gate may declare nothing; this must be a no-op config, not an error.
        var config = ProvisionOnlyBuilder().Build();

        await Assert.That(config.CaptureIntended).IsFalse();
        await Assert.That(config.ExternalSlots.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Capturing_without_a_context_fails_fast()
    {
        var builder = new CdcBuilder();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success)); // => CaptureIntended

        await Assert.That(() => builder.Build()).Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task ForEntity_without_a_context_fails_fast()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("elt", s => s.ForEntity<Product>());

        await Assert.That(() => builder.Build()).Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task Two_provision_only_external_slots_sharing_a_publication_fail_fast()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("a", s => s.ForTable("orders").WithPublication("shared"));
        builder.AddExternalSlot("b", s => s.ForTable("customers").WithPublication("shared"));

        await Assert.That(() => builder.Build()).Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task AddExternalSlot_is_recorded_with_explicit_publication()
    {
        var builder = MinimalBuilder();
        builder.AddExternalSlot("elt", s => s.ForTable("public", "orders").WithPublication("elt_pub"));

        var config = builder.Build();

        await Assert.That(config.ExternalSlots.Count).IsEqualTo(1);
        await Assert.That(config.ExternalSlots[0].SlotName).IsEqualTo("elt");
        await Assert.That(config.ExternalSlots[0].PublicationName).IsEqualTo("elt_pub");
    }

    [Test]
    public async Task External_slot_without_tables_fails_fast()
    {
        var builder = MinimalBuilder();
        builder.AddExternalSlot("elt", _ => { });

        await Assert.That(() => builder.Build()).Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task External_slot_name_colliding_with_primary_fails_fast()
    {
        var builder = MinimalBuilder();
        builder.ConfigureOptions(o => o.SlotName = "dup");
        builder.AddExternalSlot("dup", s => s.ForTable("orders"));

        await Assert.That(() => builder.Build()).Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task External_publication_colliding_with_primary_fails_fast()
    {
        var builder = MinimalBuilder();
        builder.ConfigureOptions(o => o.PublicationName = "shared_pub");
        builder.AddExternalSlot("elt", s => s.ForTable("orders").WithPublication("shared_pub"));

        await Assert.That(() => builder.Build()).Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task Two_external_slots_with_the_same_name_fail_fast()
    {
        var builder = MinimalBuilder();
        builder.AddExternalSlot("elt", s => s.ForTable("orders"));
        builder.AddExternalSlot("elt", s => s.ForTable("customers"));

        await Assert.That(() => builder.Build()).Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task Resolver_resolves_entity_and_string_tables_and_defaults_publication()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.TableNames.Add(("public", "products"));
        registration.EntityTypes.Add(typeof(Order)); // maps to sales.orders

        var specs = ExternalSlotResolver.Resolve(new[] { registration }, ctx.Model);

        await Assert.That(specs.Count).IsEqualTo(1);
        await Assert.That(specs[0].PublicationName).IsEqualTo("elt_pub"); // defaulted from slot name
        var tables = specs[0].Tables.Select(t => $"{t.Schema}.{t.Table}").OrderBy(n => n).ToList();
        await Assert.That(tables).IsEquivalentTo(new[] { "public.products", "sales.orders" });
    }

    [Test]
    public async Task Resolver_dedupes_a_table_declared_by_both_name_and_entity()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.TableNames.Add(("public", "products"));
        registration.EntityTypes.Add(typeof(Product)); // also public.products

        var specs = ExternalSlotResolver.Resolve(new[] { registration }, ctx.Model);

        await Assert.That(specs[0].Tables.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Resolver_throws_for_unmapped_entity()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.EntityTypes.Add(typeof(ExternalSlotConfigTests)); // not in the EF model

        await Assert.That(() => ExternalSlotResolver.Resolve(new[] { registration }, ctx.Model))
            .Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task Resolver_resolves_string_tables_without_a_model()
    {
        // Provision-only: no EF model available, ForTable(...) declarations still resolve.
        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.TableNames.Add(("public", "orders"));

        var specs = ExternalSlotResolver.Resolve(new[] { registration }, model: null);

        await Assert.That(specs.Count).IsEqualTo(1);
        await Assert.That(specs[0].PublicationName).IsEqualTo("elt_pub");
        var tables = specs[0].Tables.Select(t => $"{t.Schema}.{t.Table}").ToList();
        await Assert.That(tables).IsEquivalentTo(new[] { "public.orders" });
    }

    [Test]
    public async Task Resolver_throws_for_ForEntity_without_a_model()
    {
        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.EntityTypes.Add(typeof(Product));

        await Assert.That(() => ExternalSlotResolver.Resolve(new[] { registration }, model: null))
            .Throws<CdcConfigurationException>();
    }
}
