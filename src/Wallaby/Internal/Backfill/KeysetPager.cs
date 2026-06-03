using System.Text;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Internal.Backfill;

/// <summary>One page of backfill rows plus the cursor to resume after it.</summary>
internal sealed record BackfillChunk(IReadOnlyList<RawChange> Rows, object?[]? NextCursor, bool HasMore);

/// <summary>
/// An optional <c>WHERE</c> filter ANDed into every page a <see cref="KeysetPager"/> reads — used to
/// restrict a snapshot to the rows affected by a dependent fan-out. <see cref="PredicateSql"/> references
/// <c>@f0..@fN</c> placeholders matched positionally to <see cref="Parameters"/>.
/// </summary>
internal sealed record KeysetFilter(string PredicateSql, IReadOnlyList<object?> Parameters)
{
    /// <summary>
    /// Build an <c>IN</c>-list filter matching <paramref name="columns"/> against the distinct value
    /// <paramref name="tuples"/> (each tuple has one value per column). Single-column lookups produce
    /// <c>"col" IN (@f0, @f1, …)</c>; composite lookups produce a row-value list
    /// <c>("a","b") IN ((@f0,@f1), (@f2,@f3), …)</c>. An <c>IN</c>-list (rather than <c>= ANY(array)</c>)
    /// keeps parameter typing simple and uniform across single/composite keys and persisted-then-reloaded values.
    /// </summary>
    public static KeysetFilter ForLookup(IReadOnlyList<string> columns, IReadOnlyList<object?[]> tuples)
    {
        var parameters = new List<object?>(columns.Count * tuples.Count);
        var sql = new StringBuilder();

        if (columns.Count == 1)
        {
            var col = PgExec.QuoteIdentifier(columns[0]);
            sql.Append(col).Append(" IN (");
            for (var i = 0; i < tuples.Count; i++)
            {
                if (i > 0) sql.Append(", ");
                sql.Append("@f").Append(parameters.Count);
                parameters.Add(tuples[i][0]);
            }
            sql.Append(')');
        }
        else
        {
            var cols = string.Join(", ", columns.Select(PgExec.QuoteIdentifier));
            sql.Append('(').Append(cols).Append(") IN (");
            for (var t = 0; t < tuples.Count; t++)
            {
                if (t > 0) sql.Append(", ");
                sql.Append('(');
                for (var c = 0; c < columns.Count; c++)
                {
                    if (c > 0) sql.Append(", ");
                    sql.Append("@f").Append(parameters.Count);
                    parameters.Add(tuples[t][c]);
                }
                sql.Append(')');
            }
            sql.Append(')');
        }

        return new KeysetFilter(sql.ToString(), parameters);
    }
}

/// <summary>
/// Reads a table in primary-key order using keyset (cursor) pagination — never OFFSET — so pages are
/// stable under concurrent writes. Rows are emitted as <see cref="ChangeAction.Read"/> changes. An
/// optional <see cref="KeysetFilter"/> restricts the scan (e.g. to a dependent fan-out's affected rows).
/// </summary>
internal sealed class KeysetPager
{
    private readonly CapturedTable _table;
    private readonly KeysetFilter? _filter;
    private readonly string[] _columnNames;
    private readonly int[] _pkIndexInColumns;
    private readonly string _firstPageSqlPrefix;
    private readonly string _nextPageSqlPrefix;

    public KeysetPager(CapturedTable table, KeysetFilter? filter = null)
    {
        _table = table;
        _filter = filter;
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

        var firstWhere = filter is null ? string.Empty : $"WHERE {filter.PredicateSql} ";
        var nextWhere = filter is null
            ? $"WHERE {BuildKeysetPredicate()} "
            : $"WHERE {filter.PredicateSql} AND {BuildKeysetPredicate()} ";

        _firstPageSqlPrefix = $"SELECT {columns} {fromOrderBy}{firstWhere}ORDER BY {orderBy} LIMIT ";
        _nextPageSqlPrefix = $"SELECT {columns} {fromOrderBy}{nextWhere}ORDER BY {orderBy} LIMIT ";
    }

    public async Task<BackfillChunk> ReadChunkAsync(NpgsqlConnection connection, object?[]? cursor, int limit, CancellationToken ct)
    {
        var sql = (cursor is null ? _firstPageSqlPrefix : _nextPageSqlPrefix) + limit.ToString(System.Globalization.CultureInfo.InvariantCulture);

        await using var cmd = new NpgsqlCommand(sql, connection);
        if (_filter is not null)
        {
            for (var i = 0; i < _filter.Parameters.Count; i++)
            {
                cmd.Parameters.AddWithValue($"f{i}", _filter.Parameters[i] ?? DBNull.Value);
            }
        }
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
