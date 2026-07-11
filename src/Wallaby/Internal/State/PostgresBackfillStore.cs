using Npgsql;
using Wallaby.Abstractions;

namespace Wallaby.Internal.State;

/// <summary>Stores per-table backfill state in <c>wallaby.backfill_state</c>.</summary>
internal sealed class PostgresBackfillStore(NpgsqlDataSource dataSource) : IBackfillStateStore
{
    public async Task<BackfillState?> GetAsync(string tableQualifiedName, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand(
            "SELECT status, transform_version, cursor_json, rows_copied, updated_at FROM wallaby.backfill_state WHERE table_qualified = @t",
            connection);
        cmd.Parameters.AddWithValue("t", tableQualifiedName);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Read(reader, tableQualifiedName) : null;
    }

    public async Task<IReadOnlyList<BackfillState>> ListAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand(
            "SELECT table_qualified, status, transform_version, cursor_json, rows_copied, updated_at FROM wallaby.backfill_state",
            connection);

        var results = new List<BackfillState>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add(Read(reader, reader.GetString(0), columnOffset: 1));
        }
        return results;
    }

    public Task SaveAsync(BackfillState state, CancellationToken ct)
        => UpsertAsync(state, guardRequested: false, ct);

    public Task SaveProgressAsync(BackfillState state, CancellationToken ct)
        => UpsertAsync(state, guardRequested: true, ct);

    private async Task UpsertAsync(BackfillState state, bool guardRequested, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        // The guard makes a progress save lose to a concurrent manual request: the row stays 'Requested'
        // and the scheduler re-runs the table fresh.
        await using var cmd = new NpgsqlCommand(
            $"""
            INSERT INTO wallaby.backfill_state (table_qualified, status, transform_version, cursor_json, rows_copied, updated_at)
            VALUES (@t, @s, @v, @c::jsonb, @r, now())
            ON CONFLICT (table_qualified) DO UPDATE
                SET status = EXCLUDED.status,
                    transform_version = EXCLUDED.transform_version,
                    cursor_json = EXCLUDED.cursor_json,
                    rows_copied = EXCLUDED.rows_copied,
                    updated_at = EXCLUDED.updated_at
            {(guardRequested ? "WHERE wallaby.backfill_state.status <> 'Requested'" : string.Empty)}
            """,
            connection);
        cmd.Parameters.AddWithValue("t", state.TableQualifiedName);
        cmd.Parameters.AddWithValue("s", state.Status.ToString());
        cmd.Parameters.AddWithValue("v", (object?)state.TransformVersion ?? DBNull.Value);
        cmd.Parameters.AddWithValue("c", (object?)state.CursorJson ?? DBNull.Value);
        cmd.Parameters.AddWithValue("r", state.RowsCopied);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task RequestAsync(string tableQualifiedName, string? transformVersion, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        // The trailing pg_notify rides the same auto-committed batch, so the wake-up is delivered
        // atomically with the row becoming visible.
        await using var cmd = new NpgsqlCommand(
            $"""
            INSERT INTO wallaby.backfill_state (table_qualified, status, transform_version, cursor_json, rows_copied, updated_at)
            VALUES (@t, 'Requested', @v, NULL, 0, now())
            ON CONFLICT (table_qualified) DO UPDATE
                SET status = 'Requested',
                    transform_version = EXCLUDED.transform_version,
                    cursor_json = NULL,
                    rows_copied = 0,
                    updated_at = now();
            SELECT pg_notify('{WallabySchema.BackfillNotifyChannel}', '');
            """,
            connection);
        cmd.Parameters.AddWithValue("t", tableQualifiedName);
        cmd.Parameters.AddWithValue("v", (object?)transformVersion ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<string>> ListRequestedAsync(
        IReadOnlyList<string> tableQualifiedNames, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand(
            "SELECT table_qualified FROM wallaby.backfill_state WHERE status = 'Requested' AND table_qualified = ANY(@t)",
            connection);
        cmd.Parameters.AddWithValue("t", tableQualifiedNames.ToArray());

        var results = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add(reader.GetString(0));
        }
        return results;
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
        return new BackfillState(
            tableQualified, status, transformVersion, cursorJson, rowsCopied,
            new DateTimeOffset(DateTime.SpecifyKind(updatedAt, DateTimeKind.Utc)));
    }
}
