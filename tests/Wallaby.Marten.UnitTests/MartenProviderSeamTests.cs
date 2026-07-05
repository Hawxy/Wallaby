using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Marten;

namespace Wallaby.Marten.UnitTests;

/// <summary>
/// Proves the provider seams from a package with no EF Core dependency: UseMarten() satisfies the
/// builder's provider requirement, and the placeholder surfaces its not-implemented state at
/// capture-plan time rather than at registration.
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
    public void Registering_the_same_provider_twice_fails_fast()
    {
        var builder = CapturingBuilder();
        builder.UseMarten();

        Should.Throw<WallabyConfigurationException>(() => builder.UseMarten());
    }

    [Test]
    public void BuildCapturePlan_throws_until_the_provider_is_implemented()
    {
        var builder = CapturingBuilder();
        builder.UseMarten();
        var config = builder.Build();

        var provider = config.Providers[0].ModelProvider(new NullProvider());

        Should.Throw<NotSupportedException>(() => provider.BuildCapturePlan(
            config.ToCaptureSpec("Marten", new Dictionary<Type, string>())));
    }

    private sealed class NullProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }
}
