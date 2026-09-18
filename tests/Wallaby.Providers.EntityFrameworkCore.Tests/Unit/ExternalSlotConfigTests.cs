using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.Internal.SelfConfig;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit;

public class ExternalSlotConfigTests
{
    // A capturing builder: a sink with a mapping + a declared context, so the external-slot validation
    // runs alongside a primary slot/publication (exercises the collision-with-primary checks).
    private static WallabyBuilder MinimalBuilder()
    {
        var builder = new WallabyBuilder(new ServiceCollection());
        builder.UseEntityFrameworkCore<AppDbContext>();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success))
            .WithMappings(s => s.Map<Product>().UsingTransform((_, changes, _) =>
                Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(
                    changes.ToDictionary(c => c.Key, _ => (WallabyDocument?)null))));
        return builder;
    }

    // A provision-only builder: a connection string + external slots, no context and no sink.
    private static WallabyBuilder ProvisionOnlyBuilder()
    {
        var builder = new WallabyBuilder(new ServiceCollection());
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
        var builder = new WallabyBuilder(new ServiceCollection());
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success)); // => CaptureIntended

        Should.Throw<WallabyConfigurationException>(() => builder.Build());
    }

    [Test]
    public void ForEntity_without_a_context_fails_fast()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("elt", s => s.ForEntity<Product>());

        Should.Throw<WallabyConfigurationException>(() => builder.Build());
    }

    [Test]
    public void Two_provision_only_external_slots_sharing_a_publication_fail_fast()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("a", s => s.ForTable("orders").WithPublication("shared"));
        builder.AddExternalSlot("b", s => s.ForTable("customers").WithPublication("shared"));

        Should.Throw<WallabyConfigurationException>(() => builder.Build());
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

        Should.Throw<WallabyConfigurationException>(() => builder.Build());
    }

    // Collisions with the PRIMARY slot/publication involve option values, which are not final until the
    // options pipeline runs — so they surface on first WallabyOptions resolution rather than at Build().
    [Test]
    public void External_slot_name_colliding_with_primary_fails_on_options_resolution()
    {
        var config = MinimalBuilder()
            .ConfigureOptions(o => o.SlotName = "dup")
            .AddExternalSlot("dup", s => s.ForTable("orders"))
            .Build();

        Should.Throw<WallabyConfigurationException>(() => ValidatedOptions(config));
    }

    [Test]
    public void External_publication_colliding_with_primary_fails_on_options_resolution()
    {
        var config = MinimalBuilder()
            .ConfigureOptions(o => o.PublicationName = "shared_pub")
            .AddExternalSlot("elt", s => s.ForTable("orders").WithPublication("shared_pub"))
            .Build();

        Should.Throw<WallabyConfigurationException>(() => ValidatedOptions(config));
    }

    /// <summary>Materialize WallabyOptions the way AddWallaby does: builder actions applied, then validated.</summary>
    private static WallabyOptions ValidatedOptions(WallabyConfiguration config)
    {
        using var provider = new ServiceCollection().BuildServiceProvider();
        var options = new WallabyOptions();
        foreach (var apply in config.OptionsActions)
        {
            apply(provider, options);
        }
        var result = new WallabyOptionsValidator(config).Validate(null, options);
        return result.Failed
            ? throw new WallabyConfigurationException(string.Join(" ", result.Failures ?? []))
            : options;
    }

    [Test]
    public void Two_external_slots_with_the_same_name_fail_fast()
    {
        var builder = MinimalBuilder();
        builder.AddExternalSlot("elt", s => s.ForTable("orders"));
        builder.AddExternalSlot("elt", s => s.ForTable("customers"));

        Should.Throw<WallabyConfigurationException>(() => builder.Build());
    }

    [Test]
    public async Task Resolver_resolves_entity_and_string_tables_and_defaults_publication()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.TableNames.Add(("public", "products"));
        registration.EntityTypes.Add(typeof(Order)); // maps to sales.orders

        var specs = ExternalSlotResolver.Resolve(new[] { registration }, [("EntityFrameworkCore", new EfCoreModelProvider(ctx.Model))]);

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

        var specs = ExternalSlotResolver.Resolve(new[] { registration }, [("EntityFrameworkCore", new EfCoreModelProvider(ctx.Model))]);

        specs[0].Tables.Count.ShouldBe(1);
    }

    [Test]
    public async Task Resolver_throws_for_unmapped_entity()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.EntityTypes.Add(typeof(ExternalSlotConfigTests)); // not in the EF model

        Should.Throw<WallabyConfigurationException>(
            () => ExternalSlotResolver.Resolve(new[] { registration }, [("EntityFrameworkCore", new EfCoreModelProvider(ctx.Model))]));
    }

    [Test]
    public void Resolver_resolves_string_tables_without_a_model()
    {
        // Provision-only: no EF model available, ForTable(...) declarations still resolve.
        var registration = new ExternalSlotRegistration { SlotName = "elt" };
        registration.TableNames.Add(("public", "orders"));

        var specs = ExternalSlotResolver.Resolve(new[] { registration }, modelProviders: []);

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

        Should.Throw<WallabyConfigurationException>(
            () => ExternalSlotResolver.Resolve(new[] { registration }, modelProviders: []));
    }

    [Test]
    public void ForAllEntities_without_a_context_fails_fast()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("elt", s => s.ForAllEntities());

        Should.Throw<WallabyConfigurationException>(() => builder.Build());
    }

    [Test]
    public void Except_without_ForAllEntities_fails_fast()
    {
        var builder = MinimalBuilder();
        builder.AddExternalSlot("elt", s => s.ForTable("orders").Except<Product>());

        Should.Throw<WallabyConfigurationException>(() => builder.Build())
            .Message.ShouldContain("ForAllEntities()");
    }

    [Test]
    public void ForAllEntities_alone_satisfies_the_table_requirement()
    {
        var builder = MinimalBuilder();
        builder.AddExternalSlot("elt", s => s.ForAllEntities().Except<Product>().Except("sales", "orders"));

        var config = builder.Build();

        config.ExternalSlots[0].AllEntities.ShouldBeTrue();
        config.ExternalSlots[0].ExcludedEntityTypes.ShouldBe([typeof(Product)]);
        config.ExternalSlots[0].ExcludedTableNames.ShouldBe([("sales", "orders")]);
    }

    private static IReadOnlyList<(string Name, IWallabyModelProvider Provider)> EfProvider(AppDbContext ctx)
        => [("EntityFrameworkCore", new EfCoreModelProvider(ctx.Model))];

    private static List<string> Names(ExternalSlotSpec spec) => spec.Tables.Select(t => $"{t.Schema}.{t.Table}").ToList();

    [Test]
    public async Task Resolver_expands_ForAllEntities_to_every_model_table()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var registration = new ExternalSlotRegistration { SlotName = "elt", AllEntities = true };
        registration.TableNames.Add(("legacy", "invoices"));

        var specs = ExternalSlotResolver.Resolve([registration], EfProvider(ctx));

        var names = Names(specs[0]);
        names.Count.ShouldBe(11);
        names.ShouldContain("public.products");
        names.ShouldContain("sales.order_lines");
        names.ShouldContain("public.product_labels");
        names.Last().ShouldBe("legacy.invoices"); // explicit tables follow the model's
    }

    [Test]
    public async Task Resolver_removes_excluded_entities_and_named_tables()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var registration = new ExternalSlotRegistration { SlotName = "elt", AllEntities = true };
        registration.ExcludedEntityTypes.Add(typeof(Order));
        registration.ExcludedTableNames.Add(("public", "customers"));
        registration.ExcludedTableNames.Add(("public", "product_labels")); // no CLR type: by name only

        var specs = ExternalSlotResolver.Resolve([registration], EfProvider(ctx));

        var names = Names(specs[0]);
        names.Count.ShouldBe(7);
        names.ShouldNotContain("sales.orders");
        names.ShouldNotContain("public.customers");
        names.ShouldNotContain("public.product_labels");
        names.ShouldContain("sales.order_lines");
    }

    [Test]
    public async Task Resolver_throws_for_an_exclusion_the_model_does_not_contain()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var registration = new ExternalSlotRegistration { SlotName = "elt", AllEntities = true };
        registration.ExcludedTableNames.Add(("public", "orders")); // lives in the sales schema

        var ex = Should.Throw<WallabyConfigurationException>(() => ExternalSlotResolver.Resolve([registration], EfProvider(ctx)));

        ex.Message.ShouldContain("'public.orders'");
        ex.Message.ShouldContain("Did you mean 'sales.orders'?");
    }

    [Test]
    public async Task Resolver_throws_for_an_exclusion_that_is_also_declared_explicitly()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var registration = new ExternalSlotRegistration { SlotName = "elt", AllEntities = true };
        registration.EntityTypes.Add(typeof(Product));
        registration.ExcludedTableNames.Add(("public", "products"));

        Should.Throw<WallabyConfigurationException>(() => ExternalSlotResolver.Resolve([registration], EfProvider(ctx)))
            .Message.ShouldContain("also declares");
    }

    [Test]
    public async Task Resolver_throws_when_exclusions_remove_every_table()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var registration = new ExternalSlotRegistration { SlotName = "elt", AllEntities = true };
        foreach (var table in new EfCoreModelProvider(ctx.Model).ResolveAllTables())
        {
            registration.ExcludedTableNames.Add((table.Schema, table.Table));
        }

        Should.Throw<WallabyConfigurationException>(() => ExternalSlotResolver.Resolve([registration], EfProvider(ctx)))
            .Message.ShouldContain("no tables");
    }

    [Test]
    public void Resolver_throws_for_ForAllEntities_without_a_model()
    {
        var registration = new ExternalSlotRegistration { SlotName = "elt", AllEntities = true };

        Should.Throw<WallabyConfigurationException>(() => ExternalSlotResolver.Resolve([registration], modelProviders: []))
            .Message.ShouldContain("ForAllEntities()");
    }

    [Test]
    public void An_external_slot_name_must_be_a_valid_replication_slot_name()
    {
        var builder = ProvisionOnlyBuilder();
        builder.AddExternalSlot("Elt-Slot", s => s.ForTable("orders"));

        Should.Throw<WallabyConfigurationException>(() => builder.Build())
            .Message.ShouldContain("replication slot name");
    }
}
