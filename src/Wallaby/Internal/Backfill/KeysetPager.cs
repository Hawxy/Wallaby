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
    /// Ceiling on bound parameters per filter, well under Postgres's 65,535-parameter protocol limit so
    /// an arbitrarily wide lookup set can never produce an unexecutable query.
    /// </summary>
    internal const int MaxFilterParameters = 2000;

    /// <summary>
    /// Build filters matching <paramref name="columns"/> against the distinct value
    /// <paramref name="tuples"/> (each tuple has one value per column). A single-column lookup binds one
    /// typed-array parameter — <c>"col" = ANY(@f0)</c> — so it is always exactly one filter regardless of
    /// value count. A composite lookup produces row-value lists
    /// <c>("a","b") IN ((@f0,@f1), (@f2,@f3), …)</c>, split into multiple filters so none binds more than
    /// <paramref name="maxParametersPerQuery"/> parameters; the caller runs one paged scan per filter.
    /// Null values never match an equality lookup (SQL null semantics), so single-column nulls are
    /// dropped; a lookup with nothing left yields one <c>false</c> filter matching no rows.
    /// </summary>
    public static IReadOnlyList<KeysetFilter> ForLookup(
        IReadOnlyList<string> columns, IReadOnlyList<object?[]> tuples,
        int maxParametersPerQuery = MaxFilterParameters)
    {
        if (columns.Count == 1 && ForSingleColumn(columns[0], tuples) is { } single)
        {
            return [single];
        }

        // Composite keys — and the rare single-column element type with no typed-array mapping — use
        // parameter-per-value row-value lists, split to stay under the budget.
        return ForComposite(columns, tuples, maxParametersPerQuery);
    }

    private static KeysetFilter? ForSingleColumn(string column, IReadOnlyList<object?[]> tuples)
    {
        var values = new List<object>(tuples.Count);
        foreach (var tuple in tuples)
        {
            if (tuple[0] is { } value and not DBNull)
            {
                values.Add(value);
            }
        }

        if (values.Count == 0)
        {
            return new KeysetFilter("false", []);
        }

        // All values share one CLR type (lookup values are coerced to the column's type), so a typed
        // array binds as the matching Postgres array type. The explicit type switch (instead of
        // Array.CreateInstance) keeps this AOT-compatible.
        Array? array = values[0] switch
        {
            bool => ToArray<bool>(values),
            byte => ToArray<byte>(values),
            short => ToArray<short>(values),
            int => ToArray<int>(values),
            long => ToArray<long>(values),
            decimal => ToArray<decimal>(values),
            double => ToArray<double>(values),
            float => ToArray<float>(values),
            string => ToArray<string>(values),
            Guid => ToArray<Guid>(values),
            DateTime => ToArray<DateTime>(values),
            DateTimeOffset => ToArray<DateTimeOffset>(values),
            DateOnly => ToArray<DateOnly>(values),
            _ => null,
        };

        return array is null ? null : new KeysetFilter($"{PgExec.QuoteIdentifier(column)} = ANY(@f0)", [array]);
    }

    private static T[] ToArray<T>(List<object> values)
    {
        var result = new T[values.Count];
        for (var i = 0; i < values.Count; i++)
        {
            result[i] = (T)values[i];
        }
        return result;
    }

    private static List<KeysetFilter> ForComposite(
        IReadOnlyList<string> columns, IReadOnlyList<object?[]> tuples, int maxParametersPerQuery)
    {
        var tuplesPerBatch = Math.Max(1, maxParametersPerQuery / columns.Count);
        var cols = string.Join(", ", columns.Select(PgExec.QuoteIdentifier));

        var filters = new List<KeysetFilter>();
        for (var start = 0; start < tuples.Count; start += tuplesPerBatch)
        {
            var end = Math.Min(start + tuplesPerBatch, tuples.Count);
            var parameters = new List<object?>((end - start) * columns.Count);
            var sql = new StringBuilder();
            sql.Append('(').Append(cols).Append(") IN (");
            for (var t = start; t < end; t++)
            {
                if (t > start) sql.Append(", ");
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
            filters.Add(new KeysetFilter(sql.ToString(), parameters));
        }

        if (filters.Count == 0)
        {
            filters.Add(new KeysetFilter("false", []));
        }
        return filters;
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
    private readonly ColumnReadMode[] _readModes;
    private readonly int[] _pkIndexInColumns;
    private readonly string _firstPageSqlPrefix;
    private readonly string _nextPageSqlPrefix;

    public KeysetPager(CapturedTable table, KeysetFilter? filter = null)
    {
        _table = table;
        _filter = filter;
        _columnNames = table.Columns.Select(c => c.ColumnName).ToArray();
        _readModes = table.Columns.Select(c => c.ReadMode).ToArray();

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
        RawColumn[]? lastRow = null;
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
                values[i] = new RawColumn
                {
                    ColumnName = _columnNames[i],
                    Value = ColumnValueReader.Read(reader, ordinals[i], _readModes[i]),
                };
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

            lastRow = values;
        }

        var hasMore = rows.Count == limit;
        object?[]? lastKey = null;
        if (hasMore && lastRow is not null)
        {
            lastKey = new object?[pkCount];
            for (var i = 0; i < pkCount; i++)
            {
                lastKey[i] = lastRow[_pkIndexInColumns[i]].Value;
            }
        }
        return new BackfillChunk(rows, lastKey, hasMore);
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
