using Npgsql;

namespace Wallaby.Client.Internal;

/// <summary>A <c>wallaby.backfill_state</c> row as the remote client reads it.</summary>
internal sealed record BackfillStateRow(
    string TableQualified, string Status, long RowsCopied, DateTimeOffset UpdatedAt);

/// <summary>
/// The remote client's SQL against <see cref="BackfillContract.Table"/>. The host owns the richer
/// bookkeeping (cursors, transform versions, progress guards); the client only persists requests and
/// reads status, addressing tables by schema-qualified name since it has no entity model.
/// </summary>
internal static class BackfillOperations
{
    private const string UndefinedTable = "42P01";

    /// <summary>
    /// Persist a backfill request: mark the table's row <c>Requested</c> with progress reset, and signal
    /// the backfill channel so the leader's scheduler serves it immediately. An existing row keeps its
    /// transform version (the host compares it against the deployed version) and its purge mark is
    /// sticky-OR (a pending purge survives a plain request); a table Wallaby has never backfilled gets a
    /// fresh row. Throws <c>42P01</c> when no Wallaby host has ever run.
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
