using Npgsql;

namespace Wallaby.Client.Internal;

/// <summary>A <c>wallaby.backfill_state</c> row as the remote client reads it.</summary>
internal sealed record BackfillStateRow(
    string TableQualified, string Status, long RowsCopied, DateTimeOffset UpdatedAt);

/// <summary>
/// Request/cancel/read SQL against <see cref="BackfillContract.Table"/>, shared verbatim between the
/// host and the remote client (compile-linked like <see cref="ControlOperations"/>), so "mark a table
/// Requested" has exactly one statement and one set of preservation rules. The host additionally owns
/// the run bookkeeping (fresh-run stamps, progress saves) in its own store.
/// </summary>
internal static class BackfillOperations
{
    private const string UndefinedTable = "42P01";

    /// <summary>
    /// The single request write path: manual requests, the remote client, the slot-gap repair, and the
    /// fan-out overflow all issue this statement. Marks the table's row <c>Requested</c> with progress
    /// reset and signals the backfill channel so the leader's scheduler serves it immediately. An
    /// existing row keeps its transform version (stamped only by the scheduler's fresh-run write) and
    /// its purge mark is sticky-OR (a pending purge survives a plain request); a table Wallaby has never
    /// backfilled gets a fresh row. Throws <c>42P01</c> when no Wallaby host has ever run.
    /// </summary>
    public static async Task RequestAsync(
        NpgsqlDataSource dataSource, string tableQualifiedName, bool purge, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(
            $"""
             INSERT INTO {BackfillContract.Table} (table_qualified, status, transform_version, cursor_json, rows_copied, purge, updated_at)
             VALUES (@t, '{BackfillContract.StatusRequested}', NULL, NULL, 0, @p, now())
             ON CONFLICT (table_qualified) DO UPDATE
                 SET status = '{BackfillContract.StatusRequested}', cursor_json = NULL, rows_copied = 0,
                     purge = {BackfillContract.Table}.purge OR EXCLUDED.purge, updated_at = now();
             SELECT pg_notify('{BackfillContract.NotifyChannel}', '');
             """);
        cmd.Parameters.AddWithValue("t", tableQualifiedName);
        cmd.Parameters.AddWithValue("p", purge);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Cancel a queued request: flip a <c>Requested</c> row to <c>Cancelled</c> and clear its pending
    /// purge mark. Returns false when the table has no queued request (absent, running, completed, or
    /// no host has ever run). Best-effort: a request the leader has already begun serving proceeds.
    /// </summary>
    public static async Task<bool> CancelAsync(
        NpgsqlDataSource dataSource, string tableQualifiedName, CancellationToken ct)
    {
        try
        {
            await using var cmd = dataSource.CreateCommand(
                $"""
                 UPDATE {BackfillContract.Table}
                 SET status = '{BackfillContract.StatusCancelled}', purge = false, updated_at = now()
                 WHERE table_qualified = @t AND status = '{BackfillContract.StatusRequested}'
                 """);
            cmd.Parameters.AddWithValue("t", tableQualifiedName);
            return await cmd.ExecuteNonQueryAsync(ct) > 0;
        }
        catch (PostgresException ex) when (ex.SqlState == UndefinedTable)
        {
            return false;
        }
    }

    /// <summary>Every tracked table's backfill state. Empty when the table doesn't exist (no host ever ran).</summary>
    public static async Task<IReadOnlyList<BackfillStateRow>> ListStatesAsync(
        NpgsqlDataSource dataSource, CancellationToken ct)
    {
        try
        {
            await using var cmd = dataSource.CreateCommand(
                $"""
                 SELECT table_qualified, status, rows_copied, updated_at
                 FROM {BackfillContract.Table} ORDER BY table_qualified
                 """);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var states = new List<BackfillStateRow>();
            while (await reader.ReadAsync(ct))
            {
                states.Add(new BackfillStateRow(
                    reader.GetString(0), reader.GetString(1), reader.GetInt64(2),
                    reader.GetFieldValue<DateTimeOffset>(3)));
            }
            return states;
        }
        catch (PostgresException ex) when (ex.SqlState == UndefinedTable)
        {
            return [];
        }
    }
}
