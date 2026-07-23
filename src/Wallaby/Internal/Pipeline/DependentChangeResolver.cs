using System.Diagnostics;
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
internal sealed class DependentChangeResolver(NpgsqlDataSource dataSource, WallabyModel model, WallabyInstrumentation? instrumentation = null)
{
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;

    /// <summary>Whether a change to the given table can trigger any dependent fan-out.</summary>
    public bool HasBindingFor(string schema, string tableName)
        => model.FindBindingsForDependent(schema, tableName).Count > 0;

    public async Task<IReadOnlyList<FanoutResult>> ResolveFirstPagesAsync(
        IAsyncEnumerable<RawChange> changes, int pageSize, CancellationToken ct)
    {
        if (model.DependentBindings.Count == 0)
        {
            return [];
        }

        // Group distinct lookup tuples per binding across the whole transaction so each binding resolves
        // with one consolidated query instead of one query per triggering change. Consuming the changes as a
        // stream keeps this bounded by the number of DISTINCT lookup keys (not the change count), so it works
        // for a spilled (streamed) transaction without materializing it.
        Dictionary<DependentBinding, BindingAccumulator>? perBinding = null;

        await foreach (var change in changes.WithCancellation(ct))
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

                if (acc.Seen.Add(values))
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
        var offloaded = 0;

        // One connection shared across every binding's first-page read for this transaction.
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        foreach (var (binding, acc) in perBinding)
        {
            var columns = binding.Lookup.Select(l => l.PrimaryColumn).ToArray();
            var filters = KeysetFilter.ForLookup(columns, acc.Tuples);
            var pager = new KeysetPager(binding.PrimaryTable, filters[0]);
            var chunk = await pager.ReadChunkAsync(connection, cursor: null, pageSize, ct);

            var page = ToSyntheticUpdates(chunk.Rows, acc.Representative);
            totalSynthetic += page.Count;
            _instr.RecordDependentSynthetic(binding.DependentTable.QualifiedName, page.Count);

            // A multi-filter lookup must offload even when the first filter's page ran dry: the later
            // filters' rows were never scanned inline.
            var continuation = chunk.HasMore || filters.Count > 1
                ? new ScopedFanoutSpec(binding.PrimaryTable, columns, acc.Tuples)
                : null;
            if (continuation is not null)
            {
                offloaded++;
                activity?.AddEvent(new ActivityEvent("fanout.offloaded", tags: new ActivityTagsCollection
                {
                    [WallabyInstrumentation.TableTag] = binding.PrimaryTable.QualifiedName,
                }));
            }
            results.Add(new FanoutResult(page, continuation));
        }

        activity?.SetTag("wallaby.dependent.count", totalSynthetic);
        // How many bindings' tails were handed to the fan-out queue — the scoped backfills that will
        // appear later as their own traces.
        activity?.SetTag("wallaby.fanout.offloaded", offloaded);
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

    private sealed class BindingAccumulator
    {
        public HashSet<object?[]> Seen { get; } = new(LookupTupleComparer.Instance);
        public List<object?[]> Tuples { get; } = [];
        public RawChange Representative { get; set; } = null!;
    }

    // Structural equality over lookup tuples, so deduping a tuple allocates nothing.
    private sealed class LookupTupleComparer : IEqualityComparer<object?[]>
    {
        public static readonly LookupTupleComparer Instance = new();

        public bool Equals(object?[]? x, object?[]? y)
        {
            if (ReferenceEquals(x, y))
            {
                return true;
            }
            if (x is null || y is null || x.Length != y.Length)
            {
                return false;
            }
            for (var i = 0; i < x.Length; i++)
            {
                if (!object.Equals(x[i], y[i]))
                {
                    return false;
                }
            }
            return true;
        }

        public int GetHashCode(object?[] values)
        {
            var hash = new HashCode();
            for (var i = 0; i < values.Length; i++)
            {
                hash.Add(values[i]);
            }
            return hash.ToHashCode();
        }
    }
}
