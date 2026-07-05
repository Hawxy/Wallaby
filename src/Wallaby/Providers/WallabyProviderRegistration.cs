namespace Wallaby.Providers;

/// <summary>
/// What a storage-provider package registers on the builder: the model provider that resolves the capture
/// plan, and the default enrichment-session provider transforms lease their sessions from. Registered via
/// <c>WallabyBuilder.UseProvider(...)</c> by provider extension methods (e.g.
/// <c>UseEntityFrameworkCore&lt;TContext&gt;()</c>); consumers normally never construct one directly.
/// </summary>
public sealed class WallabyProviderRegistration
{
    /// <summary>The provider's display name (e.g. <c>"EntityFrameworkCore"</c>), used in error messages.</summary>
    public required string Name { get; init; }

    /// <summary>Builds the model provider that resolves the capture plan and external-slot entity tables.</summary>
    public required Func<IServiceProvider, IWallabyModelProvider> ModelProvider { get; init; }

    /// <summary>Builds the default (unscoped) enrichment-session provider.</summary>
    public required Func<IServiceProvider, IEnrichmentSessionProvider> EnrichmentSessions { get; init; }
}
