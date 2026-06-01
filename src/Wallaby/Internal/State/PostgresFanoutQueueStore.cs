using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Internal.Backfill;

namespace Wallaby.Internal.State;

/// <summary>Stores scoped dependent fan-out jobs in <c>wallaby.fanout_queue</c>.</summary>
internal sealed class PostgresFanoutQueueStore(NpgsqlDataSource dataSource) : IFanoutQueueStore
{
    private const string DueStatuses = "('Requested', 'InProgress')";

    public async Task EnqueueAsync(ScopedFanoutSpec spec, CancellationToken ct)
    {
        var columns = spec.LookupColumns.ToArray();
        // Sort tuples by their JSON form so the same logical lookup set hashes identically regardless of
        // the order changes were encountered in — that's what lets repeat triggers coalesce.
        var sorted = spec.LookupValues
            .Select(t => (Tuple: t, Json: JsonSerializer.Serialize(t)))
            .OrderBy(x => x.Json, StringComparer.Ordinal)
            .Select(x => x.Tuple)
            .ToList();
        var valuesJson = KeysetCodec.SerializeTuples(sorted);
        var hash = Hash(spec.PrimaryTable.QualifiedName, columns, valuesJson);

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO wallaby.fanout_queue
                (table_qualified, lookup_hash, lookup_columns, lookup_values, status, cursor_json, rows_copied, requested_at, updated_at)
            VALUES (@t, @h, @cols, @vals::jsonb, 'Requested', NULL, 0, now(), now())
            ON CONFLICT (table_qualified, lookup_hash) DO UPDATE
                SET status = 'Requested',
                    lookup_values = EXCLUDED.lookup_values,
                    requested_at = now(),
                    updated_at = now()
            """,
            connection);
        cmd.Parameters.AddWithValue("t", spec.PrimaryTable.QualifiedName);
        cmd.Parameters.AddWithValue("h", hash);
        cmd.Parameters.AddWithValue("cols", columns);
        cmd.Parameters.AddWithValue("vals", valuesJson);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<FanoutJobRow?> GetNextDueAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            $"""
            SELECT table_qualified, lookup_hash, status, lookup_columns, lookup_values, cursor_json, rows_copied
            FROM wallaby.fanout_queue
            WHERE status IN {DueStatuses}
            ORDER BY requested_at
            LIMIT 1
            """,
            connection);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Map(reader) : null;
    }

    public Task MarkInProgressAsync(string tableQualified, string lookupHash, string? startCursorJson, CancellationToken ct)
        => PgExec.ExecuteAsync(
            dataSource,
            """
            UPDATE wallaby.fanout_queue
            SET status = 'InProgress', cursor_json = @c::jsonb, updated_at = now()
            WHERE table_qualified = @t AND lookup_hash = @h
            """,
            ct,
            ("t", tableQualified), ("h", lookupHash), ("c", (object?)startCursorJson ?? DBNull.Value));

    public Task SaveProgressAsync(string tableQualified, string lookupHash, string? cursorJson, long rowsCopied, CancellationToken ct)
        => PgExec.ExecuteAsync(
            dataSource,
            """
            UPDATE wallaby.fanout_queue
            SET cursor_json = @c::jsonb, rows_copied = @r, updated_at = now()
            WHERE table_qualified = @t AND lookup_hash = @h
            """,
            ct,
            ("t", tableQualified), ("h", lookupHash), ("c", (object?)cursorJson ?? DBNull.Value), ("r", rowsCopied));

    public Task CompleteAsync(string tableQualified, string lookupHash, CancellationToken ct)
        => PgExec.ExecuteAsync(
            dataSource,
            """
            UPDATE wallaby.fanout_queue
            SET status = 'Completed', updated_at = now()
            WHERE table_qualified = @t AND lookup_hash = @h AND status = 'InProgress'
            """,
            ct,
            ("t", tableQualified), ("h", lookupHash));

    public async Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT table_qualified, lookup_hash, status, lookup_columns, lookup_values, cursor_json, rows_copied FROM wallaby.fanout_queue",
            connection);

        var results = new List<FanoutJobRow>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add(Map(reader));
        }
        return results;
    }

    private static FanoutJobRow Map(NpgsqlDataReader reader)
        => new(
            reader.GetString(0),
            reader.GetString(1),
            Enum.Parse<BackfillStatus>(reader.GetString(2)),
            reader.GetFieldValue<string[]>(3),
            reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.GetInt64(6));

    private static string Hash(string table, string[] columns, string valuesJson)
    {
        var canonical = string.Concat(table, "|", string.Join("|", columns), "|", valuesJson);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
    }
}
