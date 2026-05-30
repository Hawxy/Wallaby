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
internal sealed class KeysetPager(NpgsqlDataSource dataSource, CapturedTable table)
{
    public async Task<BackfillChunk> ReadChunkAsync(object?[]? cursor, int limit, CancellationToken ct)
    {
        var columns = string.Join(", ", table.Columns.Select(c => PgExec.QuoteIdentifier(c.ColumnName)));
        var orderBy = string.Join(", ", table.PrimaryKey.Select(c => PgExec.QuoteIdentifier(c.ColumnName)));
        var where = cursor is null ? string.Empty : "WHERE " + BuildKeysetPredicate();

        var sql = $"SELECT {columns} FROM {PgExec.QuoteTable(table.Schema, table.TableName)} {where} ORDER BY {orderBy} LIMIT {limit}";

        await using var connection = await dataSource.OpenConnectionAsync(ct);
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

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new RawColumn[table.Columns.Count];
            for (var i = 0; i < table.Columns.Count; i++)
            {
                var raw = reader.GetValue(reader.GetOrdinal(table.Columns[i].ColumnName));
                values[i] = new RawColumn { ColumnName = table.Columns[i].ColumnName, Value = raw is DBNull ? null : raw };
            }

            rows.Add(new RawChange
            {
                RelationId = 0,
                Schema = table.Schema,
                TableName = table.TableName,
                Action = ChangeAction.Read,
                NewValues = values,
                OldValues = null,
            });

            lastKey = table.PrimaryKey
                .Select(pk => reader.GetValue(reader.GetOrdinal(pk.ColumnName)) is var v && v is DBNull ? null : v)
                .ToArray();
        }

        var hasMore = rows.Count == limit;
        return new BackfillChunk(rows, hasMore ? lastKey : null, hasMore);
    }

    private string BuildKeysetPredicate()
    {
        var pkColumns = table.PrimaryKey.Select(c => PgExec.QuoteIdentifier(c.ColumnName)).ToList();
        var parameters = Enumerable.Range(0, table.PrimaryKey.Count).Select(i => $"@p{i}").ToList();

        // Single column: "col" > @p0 ; composite: row-value comparison ("a","b") > (@p0,@p1).
        return pkColumns.Count == 1
            ? $"{pkColumns[0]} > {parameters[0]}"
            : $"({string.Join(", ", pkColumns)}) > ({string.Join(", ", parameters)})";
    }
}
