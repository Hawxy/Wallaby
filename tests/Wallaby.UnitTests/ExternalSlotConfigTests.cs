using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Internal.SelfConfig;
using Wallaby.TestModel;

namespace Wallaby.UnitTests;

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
    public void Provision_only_without_a_sink_or_context_is_valid()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("elt", s => s.ForTable("orders"));

        var config = builder.Build();

        config.CaptureIntended.ShouldBeFalse();
        config.ExternalSlots.Count.ShouldBe(1);
    }

    [Test]
    public void Provision_only_with_no_external_slots_is_valid()
    {
        // The consumer's env gate may declare nothing; this must be a no-op config, not an error.
        var config = ProvisionOnlyBuilder().Build();

        config.CaptureIntended.ShouldBeFalse();
        config.ExternalSlots.Count.ShouldBe(0);
    }

    [Test]
    public void Capturing_without_a_context_fails_fast()
    {
        var builder = new CdcBuilder();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success)); // => CaptureIntended

        Should.Throw<CdcConfigurationException>(() => builder.Build());
    }

    [Test]
    public void ForEntity_without_a_context_fails_fast()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("elt", s => s.ForEntity<Product>());

        Should.Throw<CdcConfigurationException>(() => builder.Build());
    }

    [Test]
    public void Two_provision_only_external_slots_sharing_a_publication_fail_fast()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("a", s => s.ForTable("orders").WithPublication("shared"));
        builder.AddExternalSlot("b", s => s.ForTable("customers").WithPublication("shared"));

        Should.Throw<CdcConfigurationException>(() => builder.Build());
    }

    [Test]
    public void AddExternalSlot_is_recorded_with_explicit_publication()
    {
        var builder = MinimalBuilder();
        builder.AddExternalSlot("elt", s => s.ForTable("public", "orders").WithPublication("elt_pub"));

        var config = builder.Build();

        config.ExternalSlots.Count.ShouldBe(1);
        config.ExternalSlots[0].SlotName.ShouldBe("elt");
        config.ExternalSlots[0].PublicationName.ShouldBe("elt_pub");
    }

    [Test]
    public void External_slot_without_tables_fails_fast()
    {
        var builder = MinimalBuilder();
        builder.AddExternalSlot("elt", _ => { });

        Should.Throw<CdcConfigurationException>(() => builder.Build());
    }

    // Collisions with the PRIMARY slot/publication involve option values, which are not final until the
    // options pipeline runs — so they surface on first CdcOptions resolution rather than at Build().
    [Test]
    public void External_slot_name_colliding_with_primary_fails_on_options_resolution()
    {
        var config = MinimalBuilder()
            .ConfigureOptions(o => o.SlotName = "dup")
            .AddExternalSlot("dup", s => s.ForTable("orders"))
            .Build();

        Should.Throw<CdcConfigurationException>(() => ValidatedOptions(config));
    }

    [Test]
    public void External_publication_colliding_with_primary_fails_on_options_resolution()
    {
        var config = MinimalBuilder()
            .ConfigureOptions(o => o.PublicationName = "shared_pub")
            .AddExternalSlot("elt", s => s.ForTable("orders").WithPublication("shared_pub"))
            .Build();

        Should.Throw<CdcConfigurationException>(() => ValidatedOptions(config));
    }

    /// <summary>Materialize CdcOptions the way AddWallaby does: builder actions applied, then validated.</summary>
    private static CdcOptions ValidatedOptions(CdcConfiguration config)
    {
        var options = new CdcOptions();
        foreach (var apply in config.OptionsActions)
        {
            apply(options);
        }
        var result = new CdcOptionsValidator(config).Validate(null, options);
        return result.Failed
            ? throw new CdcConfigurationException(string.Join(" ", result.Failures ?? []))
            : options;
    }

    [Test]
    public void Two_external_slots_with_the_same_name_fail_fast()
    {
        var builder = MinimalBuilder();
        builder.AddExternalSlot("elt", s => s.ForTable("orders"));
        builder.AddExternalSlot("elt", s => s.ForTable("customers"));

        Should.Throw<CdcConfigurationException>(() => builder.Build());
    }

    [Test]
    public async Task Resolver_resolves_entity_and_string_tables_and_defaults_publication()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.TableNames.Add(("public", "products"));
        registration.EntityTypes.Add(typeof(Order)); // maps to sales.orders

        var specs = ExternalSlotResolver.Resolve(new[] { registration }, ctx.Model);

        specs.Count.ShouldBe(1);
        specs[0].PublicationName.ShouldBe("elt_pub"); // defaulted from slot name
        var tables = specs[0].Tables.Select(t => $"{t.Schema}.{t.Table}").OrderBy(n => n).ToList();
        tables.ShouldBe(new[] { "public.products", "sales.orders" }, ignoreOrder: true);
    }

    [Test]
    public async Task Resolver_dedupes_a_table_declared_by_both_name_and_entity()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.TableNames.Add(("public", "products"));
        registration.EntityTypes.Add(typeof(Product)); // also public.products

        var specs = ExternalSlotResolver.Resolve(new[] { registration }, ctx.Model);

        specs[0].Tables.Count.ShouldBe(1);
    }

    [Test]
    public async Task Resolver_throws_for_unmapped_entity()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.EntityTypes.Add(typeof(ExternalSlotConfigTests)); // not in the EF model

        Should.Throw<CdcConfigurationException>(() => ExternalSlotResolver.Resolve(new[] { registration }, ctx.Model));
    }

    [Test]
    public void Resolver_resolves_string_tables_without_a_model()
    {
        // Provision-only: no EF model available, ForTable(...) declarations still resolve.
        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.TableNames.Add(("public", "orders"));

        var specs = ExternalSlotResolver.Resolve(new[] { registration }, model: null);

        specs.Count.ShouldBe(1);
        specs[0].PublicationName.ShouldBe("elt_pub");
        var tables = specs[0].Tables.Select(t => $"{t.Schema}.{t.Table}").ToList();
        tables.ShouldBe(new[] { "public.orders" }, ignoreOrder: true);
    }

    [Test]
    public void Resolver_throws_for_ForEntity_without_a_model()
    {
        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.EntityTypes.Add(typeof(Product));

        Should.Throw<CdcConfigurationException>(() => ExternalSlotResolver.Resolve(new[] { registration }, model: null));
    }
}
