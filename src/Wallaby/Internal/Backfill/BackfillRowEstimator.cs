using Microsoft.Extensions.Logging;
using Npgsql;
using Wallaby.Model;

namespace Wallaby.Internal.Backfill;

/// <summary>Estimates how many rows a fresh backfill of a table will read; null when unknown.</summary>
internal interface IBackfillRowEstimator
{
    Task<long?> EstimateAsync(CapturedTable table, CancellationToken ct);
}

/// <summary>
/// Reads the planner's row estimate (<c>pg_class.reltuples</c>) for the table and every descendant, so a
/// partitioned root is estimated from its partitions. A never-analysed relation reports -1 and counts as
/// zero; when nothing under the table has been analysed the estimate is unknown. Any failure is logged
/// and yields unknown: an estimate never blocks a backfill.
/// </summary>
internal sealed partial class PostgresBackfillRowEstimator(NpgsqlDataSource dataSource, ILogger logger) : IBackfillRowEstimator
{
    private readonly ILogger _logger = logger;

    private const string Sql = """
        WITH RECURSIVE rel AS (
            SELECT c.oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = @schema AND c.relname = @table
            UNION ALL
            SELECT i.inhrelid FROM pg_inherits i JOIN rel ON i.inhparent = rel.oid
        )
        SELECT coalesce(sum(greatest(c.reltuples, 0)), 0)::bigint AS estimate,
               bool_or(c.reltuples >= 0) AS analysed
        FROM rel JOIN pg_class c ON c.oid = rel.oid
        """;

    public async Task<long?> EstimateAsync(CapturedTable table, CancellationToken ct)
    {
        try
        {
            await using var connection = await dataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand(Sql, connection);
            cmd.Parameters.AddWithValue("schema", table.Schema);
            cmd.Parameters.AddWithValue("table", table.TableName);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct) || reader.IsDBNull(1) || !reader.GetBoolean(1))
            {
                return null;
            }
            return reader.GetInt64(0);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            BackfillEstimateFailed(table.QualifiedName, ex);
            return null;
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not estimate the row count of {Table} for backfill progress; the backfill runs without one.")]
    private partial void BackfillEstimateFailed(string table, Exception ex);
}
