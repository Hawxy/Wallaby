using System.Collections.Concurrent;
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
    // Bindings live for the process lifetime, so a concurrent cache keyed by binding identity is
    // safe; the resolver itself is created once.
    private readonly ConcurrentDictionary<DependentBinding, BindingPlan> _planCache = new();

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

        // Index source columns once so each binding's lookup is O(K) hash probes rather than
        // O(K * sourceWidth) linear scans.
        var sourceByName = new Dictionary<string, RawColumn>(source.Count);
        for (var i = 0; i < source.Count; i++)
        {
            sourceByName[source[i].ColumnName] = source[i];
        }

        var synthetic = new List<RawChange>();
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        foreach (var binding in bindings)
        {
            if (!TryExtractLookup(sourceByName, binding, out var values))
            {
                continue;
            }

            var plan = _planCache.GetOrAdd(binding, BuildPlan);
            await ResolveOneAsync(connection, change, plan, values, synthetic, ct);
        }

        return synthetic;
    }

    private static bool TryExtractLookup(
        Dictionary<string, RawColumn> source, DependentBinding binding, out object?[] values)
    {
        values = new object?[binding.Lookup.Count];
        for (var i = 0; i < binding.Lookup.Count; i++)
        {
            if (!source.TryGetValue(binding.Lookup[i].DependentColumn, out var column) || column.IsUnchangedToast)
            {
                return false;
            }
            values[i] = column.Value;
        }
        return true;
    }

    private static BindingPlan BuildPlan(DependentBinding binding)
    {
        var primary = binding.PrimaryTable;
        var columnNames = primary.Columns.Select(c => c.ColumnName).ToArray();
        var columnList = string.Join(", ", columnNames.Select(PgExec.QuoteIdentifier));
        var whereClause = string.Join(
            " AND ",
            binding.Lookup.Select((l, i) => $"{PgExec.QuoteIdentifier(l.PrimaryColumn)} = @p{i}"));
        var sql = $"SELECT {columnList} FROM {PgExec.QuoteTable(primary.Schema, primary.TableName)} WHERE {whereClause}";
        return new BindingPlan(primary.Schema, primary.TableName, columnNames, sql);
    }

    private static async Task ResolveOneAsync(
        NpgsqlConnection connection,
        RawChange source,
        BindingPlan plan,
        object?[] values,
        List<RawChange> synthetic,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(plan.Sql, connection);
        for (var i = 0; i < values.Length; i++)
        {
            cmd.Parameters.AddWithValue($"p{i}", values[i] ?? DBNull.Value);
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        var columnCount = plan.ColumnNames.Length;
        var ordinals = new int[columnCount];
        for (var i = 0; i < columnCount; i++)
        {
            ordinals[i] = reader.GetOrdinal(plan.ColumnNames[i]);
        }

        while (await reader.ReadAsync(ct))
        {
            var newValues = new RawColumn[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                var raw = reader.GetValue(ordinals[i]);
                newValues[i] = new RawColumn
                {
                    ColumnName = plan.ColumnNames[i],
                    Value = raw is DBNull ? null : raw,
                };
            }

            synthetic.Add(new RawChange
            {
                RelationId = 0,
                Schema = plan.Schema,
                TableName = plan.TableName,
                Action = ChangeAction.Update,
                NewValues = newValues,
                OldValues = null,
                CommitLsn = source.CommitLsn,
                CommitTimestamp = source.CommitTimestamp,
                CommitIdx = source.CommitIdx,
            });
        }
    }

    private sealed record BindingPlan(string Schema, string TableName, string[] ColumnNames, string Sql);
}
