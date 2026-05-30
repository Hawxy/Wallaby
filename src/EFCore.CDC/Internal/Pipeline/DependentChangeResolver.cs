using EFCore.CDC.Abstractions;
using EFCore.CDC.Model;
using Npgsql;

namespace EFCore.CDC.Internal.Pipeline;

/// <summary>
/// Turns a change to a <em>dependent</em> table (a principal of a reference navigation or a join table
/// for a skip-navigation) into synthetic <c>Update</c> changes against the affected primary table rows,
/// using the <see cref="DependentBinding"/> lookups in the <see cref="CdcModel"/> and a fresh
/// <c>SELECT</c> against the source database. Returned <see cref="RawChange"/>s carry the originating
/// transaction's commit metadata so the pipeline's ordering and watermark accounting are preserved.
/// </summary>
internal sealed class DependentChangeResolver(NpgsqlDataSource dataSource, CdcModel model)
{
    public async Task<IReadOnlyList<RawChange>> ResolveAsync(RawChange change, CancellationToken ct)
    {
        var bindings = model.FindBindingsForDependent(change.Schema, change.TableName);
        if (bindings.Count == 0)
        {
            return [];
        }

        // For deletes the row's columns ride in OldValues (per the table's REPLICA IDENTITY).
        var source = change.Action == ChangeAction.Delete ? change.OldValues : change.NewValues;
        if (source is null || source.Count == 0)
        {
            return [];
        }

        var synthetic = new List<RawChange>();
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        foreach (var binding in bindings)
        {
            if (!TryExtractLookup(source, binding, out var values))
            {
                continue;
            }

            await ResolveOneAsync(connection, change, binding, values, synthetic, ct);
        }

        return synthetic;
    }

    private static bool TryExtractLookup(IReadOnlyList<RawColumn> source, DependentBinding binding, out object?[] values)
    {
        values = new object?[binding.Lookup.Count];
        for (var i = 0; i < binding.Lookup.Count; i++)
        {
            var name = binding.Lookup[i].DependentColumn;
            RawColumn? column = null;
            foreach (var candidate in source)
            {
                if (candidate.ColumnName == name)
                {
                    column = candidate;
                    break;
                }
            }
            if (column is null || column.IsUnchangedToast)
            {
                return false;
            }
            values[i] = column.Value;
        }
        return true;
    }

    private static async Task ResolveOneAsync(
        NpgsqlConnection connection,
        RawChange source,
        DependentBinding binding,
        object?[] values,
        List<RawChange> synthetic,
        CancellationToken ct)
    {
        var primary = binding.PrimaryTable;
        var columnList = string.Join(", ", primary.Columns.Select(c => PgExec.QuoteIdentifier(c.ColumnName)));
        var whereClause = string.Join(
            " AND ",
            binding.Lookup.Select((l, i) => $"{PgExec.QuoteIdentifier(l.PrimaryColumn)} = @p{i}"));
        var sql = $"SELECT {columnList} FROM {PgExec.QuoteTable(primary.Schema, primary.TableName)} WHERE {whereClause}";

        await using var cmd = new NpgsqlCommand(sql, connection);
        for (var i = 0; i < values.Length; i++)
        {
            cmd.Parameters.AddWithValue($"p{i}", values[i] ?? DBNull.Value);
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var newValues = new RawColumn[primary.Columns.Count];
            for (var i = 0; i < primary.Columns.Count; i++)
            {
                var raw = reader.GetValue(reader.GetOrdinal(primary.Columns[i].ColumnName));
                newValues[i] = new RawColumn
                {
                    ColumnName = primary.Columns[i].ColumnName,
                    Value = raw is DBNull ? null : raw,
                };
            }

            synthetic.Add(new RawChange
            {
                RelationId = 0,
                Schema = primary.Schema,
                TableName = primary.TableName,
                Action = ChangeAction.Update,
                NewValues = newValues,
                OldValues = null,
                CommitLsn = source.CommitLsn,
                CommitTimestamp = source.CommitTimestamp,
                CommitIdx = source.CommitIdx,
            });
        }
    }
}
