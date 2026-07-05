using Wallaby.Abstractions;

namespace Wallaby.Providers;

/// <summary>
/// Non-generic adapter the router uses to invoke a provider-typed transform over a batch of change events.
/// Provider packages build these from their typed transform interfaces (registered via
/// <c>EntityMapBuilder.UsingTransformInvoker(...)</c>) and downcast the leased session to their
/// session type.
/// </summary>
public interface IWallabyTransformInvoker
{
    /// <summary>
    /// Invoke the transform over <paramref name="changes"/> with the leased enrichment
    /// <paramref name="session"/> (see <see cref="IEnrichmentSession.Session"/>).
    /// </summary>
    Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
        object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct);
}
