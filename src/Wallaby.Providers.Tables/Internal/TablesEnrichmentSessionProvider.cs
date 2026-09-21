using Npgsql;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>
/// Hands transforms the provider's <see cref="NpgsqlDataSource"/>. Connections are pooled and opened
/// inside the transform, so a lease owns nothing and disposal is a no-op.
/// </summary>
internal sealed class TablesEnrichmentSessionProvider(NpgsqlDataSource dataSource) : IEnrichmentSessionProvider
{
    private readonly TablesEnrichmentSession _session = new(dataSource);

    public bool IsScoped => false;

    public IEnrichmentSession Lease(object? scopeKey) => _session;
}

internal sealed class TablesEnrichmentSession(NpgsqlDataSource dataSource) : IEnrichmentSession
{
    public object Session => dataSource;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
