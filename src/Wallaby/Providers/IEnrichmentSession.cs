namespace Wallaby.Providers;

/// <summary>
/// A leased provider session (e.g. an EF Core <c>DbContext</c> or a Marten query session) handed opaquely
/// to transform invokers. Disposing the lease releases the session and any lifetime it owns (e.g. a DI scope).
/// </summary>
public interface IEnrichmentSession : IAsyncDisposable
{
    /// <summary>The provider-typed session object; the provider's transform invoker downcasts it.</summary>
    object Session { get; }
}
