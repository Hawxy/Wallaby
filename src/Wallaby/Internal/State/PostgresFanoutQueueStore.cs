using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using Wallaby.Internal.Backfill;

namespace Wallaby.Internal.State;

/// <summary>Stores scoped dependent fan-out jobs in <c>wallaby.fanout_queue</c>.</summary>
internal sealed class PostgresFanoutQueueStore(NpgsqlDataSource dataSource) : IFanoutQueueStore
{
    private const string DueStatuses = "('Requested', 'InProgress')";

    public async Task EnqueueAsync(ScopedFanoutSpec spec, CancellationToken ct)
    {
        var columns = spec.LookupColumns.ToArray();
        var valuesJson = CanonicalValuesJson(spec.LookupValues);
        var hash = Hash(spec.PrimaryTable.QualifiedName, columns, valuesJson);

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        // The trailing pg_notify wakes the worker's LISTEN connection the instant this commits, so it drains
        // without waiting for the fallback poll. It rides the same (auto-committed) statement batch, so the
        // notification is delivered atomically with the row becoming visible.
        await using var cmd = new NpgsqlCommand(
            $"""
            INSERT INTO wallaby.fanout_queue
                (table_qualified, lookup_hash, lookup_columns, lookup_values, status, cursor_json, rows_copied, requested_at, updated_at)
            VALUES (@t, @h, @cols, @vals::jsonb, 'Requested', NULL, 0, now(), now())
            ON CONFLICT (table_qualified, lookup_hash) DO UPDATE
                SET status = 'Requested',
                    lookup_values = EXCLUDED.lookup_values,
                    requested_at = now(),
                    updated_at = now(),
                    -- A fresh trigger clears any backoff left by earlier failures.
                    attempts = 0,
                    next_attempt_at = now(),
                    last_error = NULL;
            SELECT pg_notify('{WallabySchema.FanoutNotifyChannel}', '');
            """,
            connection);
        cmd.Parameters.AddWithValue("t", spec.PrimaryTable.QualifiedName);
        cmd.Parameters.AddWithValue("h", hash);
        cmd.Parameters.AddWithValue("cols", columns);
        cmd.Parameters.AddWithValue("vals", valuesJson);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public INotifySubscription Subscribe()
        => new PostgresChannelSubscription(dataSource, WallabySchema.FanoutNotifyChannel);

    public async Task<FanoutJobRow?> GetNextDueAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            $"""
            SELECT table_qualified, lookup_hash, status, lookup_columns, lookup_values, cursor_json, rows_copied, attempts
            FROM wallaby.fanout_queue
            WHERE status IN {DueStatuses} AND next_attempt_at <= now()
            ORDER BY next_attempt_at, requested_at
            LIMIT 1
            """,
            connection);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Map(reader) : null;
    }

    public async Task<long> CountDueAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            $"SELECT count(*) FROM wallaby.fanout_queue WHERE status IN {DueStatuses}", connection);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    // No status filter: every surviving row is pending (finished jobs are deleted on completion).
    public async Task<int> MaxAttemptsAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return (int)await PgExec.ScalarLongAsync(
            connection, "SELECT coalesce(max(attempts), 0) FROM wallaby.fanout_queue", ct);
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
            DELETE FROM wallaby.fanout_queue
            WHERE table_qualified = @t AND lookup_hash = @h AND status = 'InProgress'
            """,
            ct,
            ("t", tableQualified), ("h", lookupHash));

    public Task DeferAsync(string tableQualified, string lookupHash, TimeSpan delay, CancellationToken ct)
        => PgExec.ExecuteAsync(
            dataSource,
            """
            UPDATE wallaby.fanout_queue
            SET next_attempt_at = now() + @d, updated_at = now()
            WHERE table_qualified = @t AND lookup_hash = @h
            """,
            ct,
            ("t", tableQualified), ("h", lookupHash), ("d", delay));

    public Task FailAsync(string tableQualified, string lookupHash, string error, CancellationToken ct)
        => PgExec.ExecuteAsync(
            dataSource,
            """
            UPDATE wallaby.fanout_queue
            SET attempts = attempts + 1,
                last_error = @e,
                next_attempt_at = now() + least(@base * power(2, least(attempts, 16)), @max),
                updated_at = now()
            WHERE table_qualified = @t AND lookup_hash = @h
            """,
            ct,
            ("t", tableQualified), ("h", lookupHash), ("e", error),
            ("base", FailureBackoff.BaseDelay), ("max", FailureBackoff.MaxDelay));

    public async Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT table_qualified, lookup_hash, status, lookup_columns, lookup_values, cursor_json, rows_copied, attempts FROM wallaby.fanout_queue",
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
            Enum.Parse<FanoutJobStatus>(reader.GetString(2)),
            reader.GetFieldValue<string[]>(3),
            reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.GetInt64(6),
            reader.GetInt32(7));

    /// <summary>
    /// The canonical JSON for a lookup set: tuples sorted by their serialized form so the same logical
    /// set produces identical bytes — and so an identical <c>lookup_hash</c>, which is what lets repeat
    /// triggers coalesce regardless of the order changes were encountered in.
    /// </summary>
    internal static string CanonicalValuesJson(IReadOnlyList<object?[]> tuples)
    {
        // Each tuple is serialized once; its JSON (closing ']' included — that bracket participates in
        // the ordinal comparison, and persisted lookup_hash values depend on the resulting order) is
        // both the sort key and the raw value stitched into the final array.
        var serialized = new string[tuples.Count];
        for (var i = 0; i < serialized.Length; i++)
        {
            serialized[i] = KeysetCodec.SerializeTuple(tuples[i]);
        }
        Array.Sort(serialized, string.CompareOrdinal);

        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var tupleJson in serialized)
            {
                writer.WriteRawValue(tupleJson);
            }
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    internal static string Hash(string table, string[] columns, string valuesJson)
    {
        var canonical = string.Concat(table, "|", string.Join("|", columns), "|", valuesJson);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
    }
}
