using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers;

namespace Wallaby.Providers.Marten.Tests.Unit;

/// <summary>
/// Registration seams from a package with no EF Core dependency: UseMarten() satisfies the builder's
/// provider requirement, tenant sessions target the Marten registration, and the provider-typed
/// UsingTransform overloads pin mappings to the Marten provider.
/// </summary>
public class MartenProviderSeamTests
{
    private static (WallabyBuilder Builder, WallabySinkBuilder Sink) CapturingBuilder()
    {
        var builder = new WallabyBuilder();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        var sink = builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success));
        return (builder, sink);
    }

    // The smallest structurally valid mapping, for tests that only assert registration facts.
    private static void MapDoc(WallabySinkBuilder sink) => sink.WithMappings(s => s
        .Map<Doc>()
        .UsingTransform((_, changes, _) => Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(
            changes.ToDictionary(c => c.Key, _ => (WallabyDocument?)null))));

    [Test]
    public void UseMarten_satisfies_the_provider_requirement()
    {
        var (builder, sink) = CapturingBuilder();
        builder.UseMarten();
        MapDoc(sink);

        var config = builder.Build();

        var registration = config.Providers.ShouldHaveSingleItem();
        registration.Name.ShouldBe("Marten");
        registration.ModelProvider.ShouldNotBeNull();
        registration.EnrichmentSessions.ShouldNotBeNull();
    }

    [Test]
    public void Registering_Marten_twice_fails_fast()
    {
        var (builder, _) = CapturingBuilder();
        builder.UseMarten();

        Should.Throw<WallabyConfigurationException>(() => builder.UseMarten());
    }

    [Test]
    public void UseTenantSessions_requires_UseMarten_first()
    {
        var (builder, _) = CapturingBuilder();

        Should.Throw<WallabyConfigurationException>(() => builder.UseTenantSessions());
    }

    [Test]
    public void UseTenantSessions_targets_the_Marten_registration()
    {
        var (builder, sink) = CapturingBuilder();
        builder.UseMarten();
        builder.UseTenantSessions();
        MapDoc(sink);

        var config = builder.Build();

        config.Providers.Single(p => p.Name == "Marten").ScopedEnrichmentSessions.ShouldNotBeNull();
    }

    [Test]
    public void UsingTransform_pins_the_mapping_to_the_Marten_provider()
    {
        var (builder, sink) = CapturingBuilder();
        builder.UseMarten();
        sink.WithMappings(s => s.Map<Doc>().UsingTransform((_, changes, _) =>
            Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(
                changes.ToDictionary(c => c.Key, _ => (WallabyDocument?)null))));

        var config = builder.Build();

        config.AllMappings.Single().ProviderName.ShouldBe("Marten");
    }

    private sealed class Doc
    {
        public Guid Id { get; set; }
    }
}
