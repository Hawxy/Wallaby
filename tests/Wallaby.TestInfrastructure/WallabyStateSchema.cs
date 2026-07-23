using Npgsql;
using Wallaby.Internal.State;

namespace Wallaby.TestInfrastructure;

/// <summary>
/// Runs the real state-schema bootstrapper, for tests that simulate a Wallaby host having run against
/// the database (e.g. client-only tests, which perform no DDL themselves).
/// </summary>
public static class WallabyStateSchema
{
    public static async Task EnsureAsync(NpgsqlDataSource dataSource)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await new StateSchemaBootstrapper().EnsureAsync(connection, CancellationToken.None);
    }
}
