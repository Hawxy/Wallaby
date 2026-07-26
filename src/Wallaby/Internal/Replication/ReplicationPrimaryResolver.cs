using Npgsql;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Resolves the primary host out of a multi-host connection string for the replication connection.
/// Npgsql rejects replication connections with multiple hosts (and its Target Session Attributes host
/// selection never runs for them), so each host is probed in order with a short-lived unpooled
/// connection and <c>SELECT pg_is_in_recovery()</c>; the first non-recovering host wins. A single-host
/// string passes through untouched. The resolved string is rebuilt from the original (credentials and
/// every other setting kept; probe settings not carried over), with any Target Session Attributes
/// cleared since a single host leaves nothing to select.
/// </summary>
internal static class ReplicationPrimaryResolver
{
    public static async Task<string> ResolveAsync(string connectionString, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (builder.Host is null || !builder.Host.Contains(','))
        {
            return connectionString;
        }

        var hosts = ParseHosts(builder.Host, builder.Port);
        List<Exception>? failures = null;
        foreach (var (host, port) in hosts)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                if (await IsPrimaryAsync(builder, host, port, ct))
                {
                    var resolved = new NpgsqlConnectionStringBuilder(connectionString)
                    {
                        Host = host,
                        Port = port,
                    };
                    resolved.Remove("Target Session Attributes");
                    return resolved.ConnectionString;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                (failures ??= []).Add(ex);
            }
        }

        throw new InvalidOperationException(
            $"None of the hosts in the connection string ({builder.Host}) is a reachable primary; " +
            "the replication connection requires one. " +
            string.Join(" ", (failures ?? []).Select(f => f.Message)));
    }

    private static async Task<bool> IsPrimaryAsync(
        NpgsqlConnectionStringBuilder original, string host, int port, CancellationToken ct)
    {
        var probe = new NpgsqlConnectionStringBuilder(original.ConnectionString)
        {
            Host = host,
            Port = port,
            Pooling = false,
            Timeout = 5,
        };
        probe.Remove("Target Session Attributes");

        await using var connection = new NpgsqlConnection(probe.ConnectionString);
        await connection.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT pg_is_in_recovery()", connection);
        return await cmd.ExecuteScalarAsync(ct) is false;
    }

    /// <summary>
    /// Split a multi-host value into (host, port) pairs: entries are comma-separated, each optionally
    /// carrying its own <c>:port</c> (bracketed IPv6 supported); an entry without one uses
    /// <paramref name="defaultPort"/>.
    /// </summary>
    internal static IReadOnlyList<(string Host, int Port)> ParseHosts(string hostValue, int defaultPort)
    {
        var result = new List<(string, int)>();
        foreach (var raw in hostValue.Split(','))
        {
            var entry = raw.Trim();
            if (entry.Length == 0)
            {
                continue;
            }

            string host;
            var port = defaultPort;
            if (entry.StartsWith('[') && entry.IndexOf(']') is > 0 and var close)
            {
                // Bracketed IPv6, e.g. [::1] or [::1]:5433. Npgsql expects the host without brackets.
                host = entry[1..close];
                if (close + 1 < entry.Length && entry[close + 1] == ':')
                {
                    port = int.Parse(entry[(close + 2)..]);
                }
            }
            else if (entry.IndexOf(':') is >= 0 and var colon && entry.LastIndexOf(':') == colon)
            {
                // Exactly one colon: host:port. More than one is an unbracketed IPv6 address.
                host = entry[..colon];
                port = int.Parse(entry[(colon + 1)..]);
            }
            else
            {
                host = entry;
            }
            result.Add((host, port));
        }
        return result;
    }
}
