using Npgsql;

namespace Wallaby.Internal;

/// <summary>
/// Minimal helpers for running ad-hoc SQL over a normal <see cref="NpgsqlConnection"/>. Used by
/// self-configuration and state persistence to avoid repetitive command boilerplate.
/// </summary>
internal static class PgExec
{
    public static async Task<int> ExecuteAsync(
        NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object? Value)[] parameters)
    {
        await using var cmd = new NpgsqlCommand(sql, connection);
        AddParameters(cmd, parameters);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Open a connection from the data source, run the command, and dispose the connection.</summary>
    public static async Task<int> ExecuteAsync(
        NpgsqlDataSource dataSource, string sql, CancellationToken ct, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return await ExecuteAsync(connection, sql, ct, parameters);
    }

    public static async Task<object?> ScalarAsync(
        NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object? Value)[] parameters)
    {
        await using var cmd = new NpgsqlCommand(sql, connection);
        AddParameters(cmd, parameters);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is DBNull ? null : result;
    }

    public static async Task<string?> ScalarStringAsync(
        NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object? Value)[] parameters)
        => (await ScalarAsync(connection, sql, ct, parameters))?.ToString();

    public static async Task<long> ScalarLongAsync(
        NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object? Value)[] parameters)
    {
        var value = await ScalarAsync(connection, sql, ct, parameters);
        return value is null ? 0L : Convert.ToInt64(value);
    }

    public static async Task<bool> ScalarBoolAsync(
        NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object? Value)[] parameters)
    {
        var value = await ScalarAsync(connection, sql, ct, parameters);
        return value is not null && Convert.ToBoolean(value);
    }

    private static void AddParameters(NpgsqlCommand cmd, (string Name, object? Value)[] parameters)
    {
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }
    }

    /// <summary>Quote a SQL identifier, doubling embedded quotes.</summary>
    public static string QuoteIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    /// <summary>Schema-qualify and quote a table name, e.g. <c>"public"."orders"</c>.</summary>
    public static string QuoteTable(string schema, string table)
        => QuoteIdentifier(schema) + "." + QuoteIdentifier(table);
}
