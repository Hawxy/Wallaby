using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers;

namespace Wallaby.Marten.UnitTests;

/// <summary>
/// Registration seams from a package with no EF Core dependency: UseMarten() satisfies the builder's
/// provider requirement, tenant sessions target the Marten registration, and the provider-typed
/// UsingTransform overloads pin mappings to the Marten provider.
/// </summary>
public class MartenProviderSeamTests
{
    private static WallabyBuilder CapturingBuilder()
    {
        var builder = new WallabyBuilder();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success));
        return builder;
    }

    [Test]
    public void UseMarten_satisfies_the_provider_requirement()
    {
        var builder = CapturingBuilder();
        builder.UseMarten();

        var config = builder.Build();

        var registration = config.Providers.ShouldHaveSingleItem();
        registration.Name.ShouldBe("Marten");
        registration.ModelProvider.ShouldNotBeNull();
        registration.EnrichmentSessions.ShouldNotBeNull();
    }

    [Test]
    public void Registering_Marten_twice_fails_fast()
    {
        var builder = CapturingBuilder();
        builder.UseMarten();

        Should.Throw<WallabyConfigurationException>(() => builder.UseMarten());
    }

    [Test]
    public void UseTenantSessions_requires_UseMarten_first()
    {
        var builder = CapturingBuilder();

        Should.Throw<WallabyConfigurationException>(() => builder.UseTenantSessions());
    }

    [Test]
    public void UseTenantSessions_targets_the_Marten_registration()
    {
        var builder = CapturingBuilder();
        builder.UseMarten();
        builder.UseTenantSessions();

        var config = builder.Build();

        config.Providers.Single(p => p.Name == "Marten").ScopedEnrichmentSessions.ShouldNotBeNull();
    }

    [Test]
    public void UsingTransform_pins_the_mapping_to_the_Marten_provider()
    {
        var builder = CapturingBuilder();
        builder.UseMarten();
        builder.Map<Doc>().ToSink("sink").UsingTransform((_, changes, _) =>
            Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(
                changes.ToDictionary(c => c.Key, _ => (WallabyDocument?)null)));

        var config = builder.Build();

        config.Mappings[typeof(Doc)].ProviderName.ShouldBe("Marten");
    }

    private sealed class Doc
    {
        public Guid Id { get; set; }
    }
}
