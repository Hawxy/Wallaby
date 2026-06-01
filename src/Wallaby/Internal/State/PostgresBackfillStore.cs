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

    public async Task SaveAsync(BackfillState state, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO wallaby.backfill_state (table_qualified, status, transform_version, cursor_json, rows_copied, updated_at)
            VALUES (@t, @s, @v, @c::jsonb, @r, now())
            ON CONFLICT (table_qualified) DO UPDATE
                SET status = EXCLUDED.status,
                    transform_version = EXCLUDED.transform_version,
                    cursor_json = EXCLUDED.cursor_json,
                    rows_copied = EXCLUDED.rows_copied,
                    updated_at = EXCLUDED.updated_at
            """,
            connection);
        cmd.Parameters.AddWithValue("t", state.TableQualifiedName);
        cmd.Parameters.AddWithValue("s", state.Status.ToString());
        cmd.Parameters.AddWithValue("v", (object?)state.TransformVersion ?? DBNull.Value);
        cmd.Parameters.AddWithValue("c", (object?)state.CursorJson ?? DBNull.Value);
        cmd.Parameters.AddWithValue("r", state.RowsCopied);
        await cmd.ExecuteNonQueryAsync(ct);
    }

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
