namespace Wallaby.Providers;

/// <summary>
/// Supplies the enrichment sessions transforms use for lookup queries, optionally scoped by a per-row
/// key (e.g. tenant id). The router caches one lease per distinct scope key within a batch and disposes
/// them at the end of the batch.
/// </summary>
public interface IEnrichmentSessionProvider
{
    /// <summary>True when sessions differ by scope key (so the router caches per key rather than once per batch).</summary>
    bool IsScoped { get; }

    /// <summary>Lease a session for the given scope key (ignored when <see cref="IsScoped"/> is false).</summary>
    IEnrichmentSession Lease(object? scopeKey);
}
