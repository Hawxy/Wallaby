using System.Text;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Model;

namespace Wallaby.Internal.Pipeline;

/// <summary>The inline first page of a binding's fan-out plus, when more rows remain, the offload spec for the tail.</summary>
internal sealed record FanoutResult(IReadOnlyList<RawChange> FirstPage, ScopedFanoutSpec? Continuation);

/// <summary>
/// Turns the dependent-table changes in a committed transaction into synthetic <c>Update</c> changes
/// against the affected primary-table rows. For each <see cref="DependentBinding"/> it consolidates the
/// distinct lookup values seen across the transaction into a single keyset-paginated query (no N+1), and
/// reads only the <em>first page</em> inline. When a binding's affected set exceeds one page, the
/// remainder is handed back as a <see cref="ScopedFanoutSpec"/> so the pipeline can offload it to a
/// scoped backfill — keeping the trigger transaction's synchronous work (and its acknowledgement) bounded.
/// </summary>
internal sealed class DependentChangeResolver(NpgsqlDataSource dataSource, CdcModel model, WallabyInstrumentation? instrumentation = null)
{
    // Separator used to build an in-memory dedup key from a lookup tuple. The unit-separator control
    // char is extremely unlikely to collide with a primary-key value's textual form.
    private const char TupleSeparator = (char)31;

    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;

    public async Task<IReadOnlyList<FanoutResult>> ResolveFirstPagesAsync(
        IReadOnlyList<RawChange> changes, int pageSize, CancellationToken ct)
    {
        if (model.DependentBindings.Count == 0)
        {
            return [];
        }

        // Group distinct lookup tuples per binding across the whole transaction so each binding resolves
        // with one consolidated query instead of one query per triggering change.
        Dictionary<DependentBinding, BindingAccumulator>? perBinding = null;

        foreach (var change in changes)
        {
            var bindings = model.FindBindingsForDependent(change.Schema, change.TableName);
            if (bindings.Count == 0)
            {
                continue;
            }

            // For deletes the row's columns ride in OldValues (per the table's REPLICA IDENTITY).
            var source = change.Action == ChangeAction.Delete ? change.OldValues : change.NewValues;
            if (source is null || source.Count == 0)
            {
                continue;
            }

            var sourceByName = IndexColumns(source);
            foreach (var binding in bindings)
            {
                if (!TryExtractLookup(sourceByName, binding, out var values))
                {
                    continue;
                }

                perBinding ??= [];
                if (!perBinding.TryGetValue(binding, out var acc))
                {
                    acc = new BindingAccumulator();
                    perBinding[binding] = acc;
                }

                if (acc.Seen.Add(TupleKey(values)))
                {
                    acc.Tuples.Add(values);
                }
                acc.Representative = change;
            }
        }

        if (perBinding is null)
        {
            return [];
        }

        var results = new List<FanoutResult>(perBinding.Count);
        using var activity = _instr.StartDependentResolve();
        var totalSynthetic = 0;

        foreach (var (binding, acc) in perBinding)
        {
            var columns = binding.Lookup.Select(l => l.PrimaryColumn).ToArray();
            var filter = KeysetFilter.ForLookup(columns, acc.Tuples);
            var pager = new KeysetPager(dataSource, binding.PrimaryTable, filter);
            var chunk = await pager.ReadChunkAsync(cursor: null, pageSize, ct);

            var page = ToSyntheticUpdates(chunk.Rows, acc.Representative);
            totalSynthetic += page.Count;
            _instr.RecordDependentSynthetic(binding.DependentTable.QualifiedName, page.Count);

            var continuation = chunk.HasMore
                ? new ScopedFanoutSpec(binding.PrimaryTable, columns, acc.Tuples)
                : null;
            results.Add(new FanoutResult(page, continuation));
        }

        activity?.SetTag("wallaby.dependent.count", totalSynthetic);
        return results;
    }

    private static List<RawChange> ToSyntheticUpdates(IReadOnlyList<RawChange> rows, RawChange representative)
    {
        var result = new List<RawChange>(rows.Count);
        foreach (var row in rows)
        {
            result.Add(new RawChange
            {
                RelationId = 0,
                Schema = row.Schema,
                TableName = row.TableName,
                Action = ChangeAction.Update,
                NewValues = row.NewValues,
                OldValues = null,
                CommitLsn = representative.CommitLsn,
                CommitTimestamp = representative.CommitTimestamp,
                CommitIdx = representative.CommitIdx,
            });
        }
        return result;
    }

    private static Dictionary<string, RawColumn> IndexColumns(IReadOnlyList<RawColumn> source)
    {
        // Index source columns once so each binding's lookup is O(K) hash probes rather than a linear scan.
        var byName = new Dictionary<string, RawColumn>(source.Count);
        for (var i = 0; i < source.Count; i++)
        {
            byName[source[i].ColumnName] = source[i];
        }
        return byName;
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

    private static string TupleKey(object?[] values)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < values.Length; i++)
        {
            if (i > 0)
            {
                sb.Append(TupleSeparator);
            }
            sb.Append(values[i]?.ToString() ?? string.Empty);
        }
        return sb.ToString();
    }

    private sealed class BindingAccumulator
    {
        public HashSet<string> Seen { get; } = [];
        public List<object?[]> Tuples { get; } = [];
        public RawChange Representative { get; set; } = null!;
    }
}
