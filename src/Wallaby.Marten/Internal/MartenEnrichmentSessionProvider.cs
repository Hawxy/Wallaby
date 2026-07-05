using Wallaby.Providers;

namespace Wallaby.Marten.Internal;

/// <summary>Preview placeholder: session leasing arrives with the functional provider.</summary>
internal sealed class MartenEnrichmentSessionProvider : IEnrichmentSessionProvider
{
    public bool IsScoped => false;

    public IEnrichmentSession Lease(object? scopeKey)
        => throw new NotSupportedException(
            "The Wallaby.Marten provider is a preview placeholder; enrichment sessions are not yet implemented.");
}
