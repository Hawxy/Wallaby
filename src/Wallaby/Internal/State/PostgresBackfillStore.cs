using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Client.Internal;

namespace Wallaby.Internal.State;

/// <summary>Stores per-table backfill state in <c>wallaby.backfill_state</c>.</summary>
internal sealed class PostgresBackfillStore(NpgsqlDataSource dataSource) : IBackfillStateStore
{
    public async Task<BackfillState?> GetAsync(string tableQualifiedName, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand(
            "SELECT status, transform_version, cursor_json, rows_copied, updated_at, purge, attempts, next_attempt_at, last_error FROM wallaby.backfill_state WHERE table_qualified = @t",
            connection);
        cmd.Parameters.AddWithValue("t", tableQualifiedName);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Read(reader, tableQualifiedName) : null;
    }

    public async Task<IReadOnlyList<BackfillState>> ListAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand(
            "SELECT table_qualified, status, transform_version, cursor_json, rows_copied, updated_at, purge, attempts, next_attempt_at, last_error FROM wallaby.backfill_state",
            connection);

        var results = new List<BackfillState>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add(Read(reader, reader.GetString(0), columnOffset: 1));
        }
        return results;
    }

    public async Task SaveAsync(BackfillState state, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        // A fresh run also starts a fresh failure ledger: attempts, backoff, and last_error reset.
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO wallaby.backfill_state (table_qualified, status, transform_version, cursor_json, rows_copied, purge, updated_at)
            VALUES (@t, @s, @v, @c::jsonb, @r, @p, now())
            ON CONFLICT (table_qualified) DO UPDATE
                SET status = EXCLUDED.status,
                    transform_version = EXCLUDED.transform_version,
                    cursor_json = EXCLUDED.cursor_json,
                    rows_copied = EXCLUDED.rows_copied,
                    purge = EXCLUDED.purge,
                    updated_at = EXCLUDED.updated_at,
                    attempts = 0,
                    next_attempt_at = now(),
                    last_error = NULL
            """,
            connection);
        cmd.Parameters.AddWithValue("t", state.TableQualifiedName);
        cmd.Parameters.AddWithValue("s", state.Status.ToString());
        cmd.Parameters.AddWithValue("v", (object?)state.TransformVersion ?? DBNull.Value);
        cmd.Parameters.AddWithValue("c", (object?)state.CursorJson ?? DBNull.Value);
        cmd.Parameters.AddWithValue("r", state.RowsCopied);
        cmd.Parameters.AddWithValue("p", state.Purge);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task SaveProgressAsync(
        string tableQualifiedName, BackfillStatus status, string? cursorJson, long rowsCopied, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        // Progress owns only progress: transform_version keeps the value the fresh run started with,
        // and a purge mark is never touched. The guard makes the save lose to a concurrent manual
        // request: the row stays 'Requested' and the scheduler re-runs the table fresh.
        await using var cmd = new NpgsqlCommand(
            """
            UPDATE wallaby.backfill_state
            SET status = @s, cursor_json = @c::jsonb, rows_copied = @r, updated_at = now()
            WHERE table_qualified = @t AND status <> 'Requested'
            """,
            connection);
        cmd.Parameters.AddWithValue("t", tableQualifiedName);
        cmd.Parameters.AddWithValue("s", status.ToString());
        cmd.Parameters.AddWithValue("c", (object?)cursorJson ?? DBNull.Value);
        cmd.Parameters.AddWithValue("r", rowsCopied);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public Task RequestAsync(string tableQualifiedName, bool purge, CancellationToken ct)
        => BackfillOperations.RequestAsync(dataSource, tableQualifiedName, purge, ct);

    public Task<bool> CancelRequestAsync(string tableQualifiedName, CancellationToken ct)
        => BackfillOperations.CancelAsync(dataSource, tableQualifiedName, ct);

    public async Task<IReadOnlyList<string>> ListRequestedAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        // Backed-off rows are excluded so a request against a failing table doesn't hot-loop the
        // scheduler; it becomes due again when the backoff expires.
        await using var cmd = new NpgsqlCommand(
            "SELECT table_qualified FROM wallaby.backfill_state WHERE status = 'Requested' AND next_attempt_at <= now()",
            connection);

        var results = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add(reader.GetString(0));
        }
        return results;
    }

    public async Task<DateTimeOffset> FailAsync(string tableQualifiedName, string error, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        // Same schedule as the fan-out queue, computed in SQL from the persisted attempt count so it
        // survives restarts and leader changes.
        await using var cmd = new NpgsqlCommand(
            """
            UPDATE wallaby.backfill_state
            SET attempts = attempts + 1,
                last_error = @e,
                next_attempt_at = now() + least(@base * power(2, least(attempts, 16)), @max),
                updated_at = now()
            WHERE table_qualified = @t
            RETURNING next_attempt_at
            """,
            connection);
        cmd.Parameters.AddWithValue("t", tableQualifiedName);
        cmd.Parameters.AddWithValue("e", error);
        cmd.Parameters.AddWithValue("base", FailureBackoff.BaseDelay);
        cmd.Parameters.AddWithValue("max", FailureBackoff.MaxDelay);

        // No row can only mean the run failed before its fresh-run save; retry after the base delay.
        var nextAttempt = await cmd.ExecuteScalarAsync(ct);
        return nextAttempt is DateTime at
            ? new DateTimeOffset(DateTime.SpecifyKind(at, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow + FailureBackoff.BaseDelay;
    }

    public Task ClearFailureAsync(string tableQualifiedName, CancellationToken ct)
        => PgExec.ExecuteAsync(
            dataSource,
            """
            UPDATE wallaby.backfill_state
            SET attempts = 0, next_attempt_at = now(), last_error = NULL, updated_at = now()
            WHERE table_qualified = @t AND (attempts <> 0 OR last_error IS NOT NULL)
            """,
            ct,
            ("t", tableQualifiedName));

    public async Task<int> MaxAttemptsAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        // Terminal rows keep their history but no longer represent pending work.
        return (int)await PgExec.ScalarLongAsync(
            connection,
            "SELECT coalesce(max(attempts), 0) FROM wallaby.backfill_state WHERE status IN ('Requested', 'InProgress')",
            ct);
    }

    public INotifySubscription Subscribe()
        => new PostgresChannelSubscription(dataSource, WallabySchema.BackfillNotifyChannel);

    private static BackfillState Read(NpgsqlDataReader reader, string tableQualified, int columnOffset = 0)
    {
        var status = Enum.Parse<BackfillStatus>(reader.GetString(columnOffset + 0));
        var transformVersion = reader.IsDBNull(columnOffset + 1) ? null : reader.GetString(columnOffset + 1);
        var cursorJson = reader.IsDBNull(columnOffset + 2) ? null : reader.GetString(columnOffset + 2);
        var rowsCopied = reader.GetInt64(columnOffset + 3);
        var updatedAt = reader.GetFieldValue<DateTime>(columnOffset + 4);
        var purge = reader.GetBoolean(columnOffset + 5);
        var attempts = reader.GetInt32(columnOffset + 6);
        var nextAttemptAt = reader.GetFieldValue<DateTime>(columnOffset + 7);
        var lastError = reader.IsDBNull(columnOffset + 8) ? null : reader.GetString(columnOffset + 8);
        return new BackfillState(
            tableQualified, status, transformVersion, cursorJson, rowsCopied,
            new DateTimeOffset(DateTime.SpecifyKind(updatedAt, DateTimeKind.Utc)), purge,
            attempts, new DateTimeOffset(DateTime.SpecifyKind(nextAttemptAt, DateTimeKind.Utc)), lastError);
    }
}
