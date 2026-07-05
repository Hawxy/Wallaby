using Wallaby.DependencyInjection;
using Wallaby.Marten.Internal;
using Wallaby.Providers;

namespace Wallaby.Marten;

/// <summary>Marten provider registration for the Wallaby builder.</summary>
public static class MartenWallabyBuilderExtensions
{
    /// <summary>
    /// Register the Marten storage provider. Preview placeholder: registration succeeds and proves the
    /// provider seams, but building a capture plan throws <see cref="NotSupportedException"/> until the
    /// provider is implemented.
    /// </summary>
    public static WallabyBuilder UseMarten(this WallabyBuilder cdc)
    {
        ArgumentNullException.ThrowIfNull(cdc);
        return cdc.UseProvider(new WallabyProviderRegistration
        {
            Name = "Marten",
            ModelProvider = _ => new MartenModelProvider(),
            EnrichmentSessions = _ => new MartenEnrichmentSessionProvider(),
        });
    }
}
