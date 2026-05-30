using EFCore.CDC.Abstractions;
using EFCore.CDC.Model;
using Npgsql;

namespace EFCore.CDC.Internal.Backfill;

/// <summary>One page of backfill rows plus the cursor to resume after it.</summary>
internal sealed record BackfillChunk(IReadOnlyList<RawChange> Rows, object?[]? NextCursor, bool HasMore);

/// <summary>
/// Reads a table in primary-key order using keyset (cursor) pagination — never OFFSET — so pages are
/// stable under concurrent writes. Rows are emitted as <see cref="ChangeAction.Read"/> changes.
/// </summary>
internal sealed class KeysetPager
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly CapturedTable _table;
    private readonly string[] _columnNames;
    private readonly int[] _pkIndexInColumns;
    private readonly string _firstPageSqlPrefix;
    private readonly string _nextPageSqlPrefix;

    public KeysetPager(NpgsqlDataSource dataSource, CapturedTable table)
    {
        _dataSource = dataSource;
        _table = table;
        _columnNames = table.Columns.Select(c => c.ColumnName).ToArray();

        // Map each primary-key column to its index in _columnNames so we can read PK values
        // straight from the row buffer without a second GetOrdinal lookup.
        _pkIndexInColumns = new int[table.PrimaryKey.Count];
        for (var i = 0; i < table.PrimaryKey.Count; i++)
        {
            var pkName = table.PrimaryKey[i].ColumnName;
            var idx = Array.IndexOf(_columnNames, pkName);
            if (idx < 0)
            {
                // PK column not in capture set — shouldn't happen, but guard anyway.
                throw new InvalidOperationException(
                    $"Primary key column '{pkName}' is not part of the captured columns for {table.Schema}.{table.TableName}.");
            }
            _pkIndexInColumns[i] = idx;
        }

        var columns = string.Join(", ", _columnNames.Select(PgExec.QuoteIdentifier));
        var orderBy = string.Join(", ", table.PrimaryKey.Select(c => PgExec.QuoteIdentifier(c.ColumnName)));
        var fromOrderBy = $"FROM {PgExec.QuoteTable(table.Schema, table.TableName)} ";
        _firstPageSqlPrefix = $"SELECT {columns} {fromOrderBy}ORDER BY {orderBy} LIMIT ";
        _nextPageSqlPrefix = $"SELECT {columns} {fromOrderBy}WHERE {BuildKeysetPredicate()} ORDER BY {orderBy} LIMIT ";
    }

    public async Task<BackfillChunk> ReadChunkAsync(object?[]? cursor, int limit, CancellationToken ct)
    {
        var sql = (cursor is null ? _firstPageSqlPrefix : _nextPageSqlPrefix) + limit.ToString(System.Globalization.CultureInfo.InvariantCulture);

        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, connection);
        if (cursor is not null)
        {
            for (var i = 0; i < cursor.Length; i++)
            {
                cmd.Parameters.AddWithValue($"p{i}", cursor[i] ?? DBNull.Value);
            }
        }

        var rows = new List<RawChange>(limit);
        object?[]? lastKey = null;
        var pkCount = _pkIndexInColumns.Length;
        var columnCount = _columnNames.Length;

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        // Ordinals are stable for the lifetime of the reader; cache them once instead of
        // calling GetOrdinal for every column on every row.
        var ordinals = new int[columnCount];
        for (var i = 0; i < columnCount; i++)
        {
            ordinals[i] = reader.GetOrdinal(_columnNames[i]);
        }

        while (await reader.ReadAsync(ct))
        {
            var values = new RawColumn[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                var raw = reader.GetValue(ordinals[i]);
                values[i] = new RawColumn { ColumnName = _columnNames[i], Value = raw is DBNull ? null : raw };
            }

            rows.Add(new RawChange
            {
                RelationId = 0,
                Schema = _table.Schema,
                TableName = _table.TableName,
                Action = ChangeAction.Read,
                NewValues = values,
                OldValues = null,
            });

            lastKey = new object?[pkCount];
            for (var i = 0; i < pkCount; i++)
            {
                lastKey[i] = values[_pkIndexInColumns[i]].Value;
            }
        }

        var hasMore = rows.Count == limit;
        return new BackfillChunk(rows, hasMore ? lastKey : null, hasMore);
    }

    private string BuildKeysetPredicate()
    {
        var pkColumns = _table.PrimaryKey.Select(c => PgExec.QuoteIdentifier(c.ColumnName)).ToList();
        var parameters = Enumerable.Range(0, _table.PrimaryKey.Count).Select(i => $"@p{i}").ToList();

        // Single column: "col" > @p0 ; composite: row-value comparison ("a","b") > (@p0,@p1).
        return pkColumns.Count == 1
            ? $"{pkColumns[0]} > {parameters[0]}"
            : $"({string.Join(", ", pkColumns)}) > ({string.Join(", ", parameters)})";
    }
}
