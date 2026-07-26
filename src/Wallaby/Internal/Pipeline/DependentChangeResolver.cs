using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Model;

namespace Wallaby.Internal.Pipeline;

/// <summary>The inline first page of a binding's fan-out.</summary>
/// <param name="FirstPage">Synthetic updates read inline for this binding.</param>
/// <param name="RebackfillTable">
/// Set when the binding's distinct lookup set exceeded the per-transaction cap: the transaction has
/// effectively rewritten the dependent table, so the whole primary table is re-snapshotted instead.
/// </param>
internal sealed record FanoutResult(
    IReadOnlyList<RawChange> FirstPage,
    CapturedTable? RebackfillTable = null);

/// <summary>
/// Turns the dependent-table changes in a committed transaction into synthetic <c>Update</c> changes
/// against the affected primary-table rows. For each <see cref="DependentBinding"/> it consolidates the
/// distinct lookup values seen across the transaction into a single keyset-paginated query (no N+1), and
/// reads only the <em>first page</em> inline; any remainder is offloaded to the fan-out queue as scoped
/// backfill jobs, keeping the trigger transaction's synchronous work (and its acknowledgement) bounded.
/// A set wider than <paramref name="chunkSize"/> is offloaded in chunk jobs <em>as it accumulates</em>,
/// so memory stays flat regardless of how many keys the transaction touches; past
/// <paramref name="maxKeysPerTransaction"/> the binding degrades to a whole-table re-snapshot.
/// </summary>
internal sealed class DependentChangeResolver(
    NpgsqlDataSource dataSource, WallabyModel model, WallabyInstrumentation? instrumentation = null,
    int maxKeysPerTransaction = 1_000_000, int chunkSize = 10_000, ILogger? logger = null)
{
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;
    private readonly ILogger _logger = logger ?? NullLogger.Instance;

    // Dependent tables already logged for an update whose old tuple couldn't supply the lookup.
    private readonly HashSet<string> _oldLookupLogged = [];

    // The Seen set is cleared once it reaches this many entries, so dedup memory stays bounded on a
    // wide fan-out. A key recurring across clearing windows yields overlapping chunk jobs, which
    // re-upsert the same rows; TotalKeys therefore counts approximate distinct keys.
    private readonly int _seenLimit = chunkSize * 5;

    /// <summary>Whether a change to the given table can trigger any dependent fan-out.</summary>
    public bool HasBindingFor(string schema, string tableName)
        => model.FindBindingsForDependent(schema, tableName).Count > 0;

    public async Task<IReadOnlyList<FanoutResult>> ResolveFirstPagesAsync(
        IAsyncEnumerable<RawChange> changes, int pageSize,
        Func<ScopedFanoutSpec, CancellationToken, Task>? enqueueTail, CancellationToken ct)
    {
        if (model.DependentBindings.Count == 0)
        {
            return [];
        }

        // Group distinct lookup tuples per binding across the whole transaction so each binding resolves
        // with one consolidated query instead of one query per triggering change. Consuming the changes as
        // a stream (and flushing full chunks to the queue as they fill) keeps memory flat even for a
        // spilled (streamed) transaction touching millions of keys.
        Dictionary<DependentBinding, BindingAccumulator>? perBinding = null;
        var chunksEnqueued = 0;

        async ValueTask AcceptAsync(DependentBinding binding, object?[] values, RawChange change)
        {
            perBinding ??= [];
            if (!perBinding.TryGetValue(binding, out var acc))
            {
                acc = new BindingAccumulator();
                perBinding[binding] = acc;
            }

            if (acc.Overflowed)
            {
                return;
            }

            if (acc.Seen.Add(values))
            {
                acc.TotalKeys++;
                if (acc.TotalKeys > maxKeysPerTransaction)
                {
                    // Effectively a rewrite of the dependent table; a whole-table re-snapshot beats
                    // queueing more chunks (any already enqueued are superseded but harmless).
                    acc.Overflow();
                    return;
                }

                acc.Tuples.Add(values);
                if (acc.Tuples.Count >= chunkSize && enqueueTail is not null)
                {
                    // Offload the full chunk now, so the key set never accumulates past chunkSize.
                    // Chunks are cut at fixed counts in the stream's deterministic order, so a
                    // redelivered transaction re-derives identical chunks and the queue coalesces
                    // them by lookup hash.
                    await enqueueTail(
                        new ScopedFanoutSpec(binding.PrimaryTable, LookupColumns(binding), [.. acc.Tuples]), ct);
                    chunksEnqueued++;
                    acc.FlushChunk(_seenLimit);
                }
            }
            acc.Representative = change;
        }

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
            Dictionary<string, RawColumn>? oldByName = null;
            foreach (var binding in bindings)
            {
                var matched = TryExtractLookup(sourceByName, binding, out var values);
                if (matched)
                {
                    await AcceptAsync(binding, values, change);
                }

                // An update that re-points the lookup affects two primary scopes: the row's new lookup
                // value and the one it left behind. Without the old tuple's value the departed scope
                // keeps its stale copy; observing it requires REPLICA IDENTITY FULL on the dependent
                // table (the default identity sends no old tuple for a non-key change).
                if (change.Action != ChangeAction.Update)
                {
                    continue;
                }
                if (change.OldValues is { Count: > 0 } oldValuesSource)
                {
                    oldByName ??= IndexColumns(oldValuesSource);
                    if (TryExtractLookup(oldByName, binding, out var oldValues))
                    {
                        if (!matched || !LookupTupleComparer.Instance.Equals(values, oldValues))
                        {
                            await AcceptAsync(binding, oldValues, change);
                        }
                        continue;
                    }
                }
                LogOldLookupUnavailable(change);
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

        // One connection shared across every binding's first-page read, opened on the first read: a
        // transaction whose every binding overflowed reads nothing inline.
        NpgsqlConnection? connection = null;
        try
        {
            foreach (var (binding, acc) in perBinding)
            {
                if (acc.Overflowed)
                {
                    // No inline page: the whole table is about to be scanned anyway.
                    activity?.AddEvent(new ActivityEvent("fanout.overflowed", tags: new ActivityTagsCollection
                    {
                        [WallabyInstrumentation.TableTag] = binding.PrimaryTable.QualifiedName,
                        ["wallaby.fanout.key.cap"] = maxKeysPerTransaction,
                    }));
                    results.Add(new FanoutResult([], binding.PrimaryTable));
                    continue;
                }

                if (acc.Flushed)
                {
                    // The scope is already streaming to the queue in chunk jobs; flush the final partial
                    // one and skip the inline page — a page over just the residual tuples would be an
                    // arbitrary slice of a scope the queue is delivering anyway.
                    if (acc.Tuples.Count > 0 && enqueueTail is not null)
                    {
                        await enqueueTail(
                            new ScopedFanoutSpec(binding.PrimaryTable, LookupColumns(binding), [.. acc.Tuples]), ct);
                        chunksEnqueued++;
                    }
                    offloaded++;
                    activity?.AddEvent(new ActivityEvent("fanout.offloaded", tags: new ActivityTagsCollection
                    {
                        [WallabyInstrumentation.TableTag] = binding.PrimaryTable.QualifiedName,
                    }));
                    continue;
                }

                connection ??= await dataSource.OpenConnectionAsync(ct);
                var columns = LookupColumns(binding);
                var filters = KeysetFilter.ForLookup(columns, acc.Tuples);
                var pager = new KeysetPager(binding.PrimaryTable, filters[0]);
                var chunk = await pager.ReadChunkAsync(connection, cursor: null, pageSize, ct);

                var page = ToSyntheticUpdates(chunk.Rows, acc.Representative);
                totalSynthetic += page.Count;
                _instr.RecordDependentSynthetic(binding.DependentTable.QualifiedName, page.Count);

                // A multi-filter lookup must offload even when the first filter's page ran dry: the later
                // filters' rows were never scanned inline.
                if (chunk.HasMore || filters.Count > 1)
                {
                    offloaded++;
                    activity?.AddEvent(new ActivityEvent("fanout.offloaded", tags: new ActivityTagsCollection
                    {
                        [WallabyInstrumentation.TableTag] = binding.PrimaryTable.QualifiedName,
                    }));
                    if (enqueueTail is not null)
                    {
                        await enqueueTail(new ScopedFanoutSpec(binding.PrimaryTable, columns, acc.Tuples), ct);
                        chunksEnqueued++;
                    }
                }
                results.Add(new FanoutResult(page));
            }
        }
        finally
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }
        }

        activity?.SetTag("wallaby.dependent.count", totalSynthetic);
        // How many bindings' tails were handed to the fan-out queue: the scoped backfills that will
        // appear later as their own traces.
        activity?.SetTag("wallaby.fanout.offloaded", offloaded);
        if (chunksEnqueued > 0)
        {
            activity?.SetTag("wallaby.fanout.chunks", chunksEnqueued);
        }
        return results;
    }

    private void LogOldLookupUnavailable(RawChange change)
    {
        if (_oldLookupLogged.Add($"{change.Schema}.{change.TableName}"))
        {
            _logger.DependentOldLookupUnavailable(change.Schema, change.TableName);
        }
    }

    private static string[] LookupColumns(DependentBinding binding)
        => [.. binding.Lookup.Select(l => l.PrimaryColumn)];

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

        /// <summary>Approximate distinct keys accepted this transaction (never reset by a chunk flush).</summary>
        public int TotalKeys { get; set; }

        /// <summary>True once at least one chunk was offloaded to the queue mid-consumption.</summary>
        public bool Flushed { get; private set; }

        /// <summary>True once the distinct-key set passed the cap and was discarded for a whole-table re-snapshot.</summary>
        public bool Overflowed { get; private set; }

        public void FlushChunk(int seenLimit)
        {
            Flushed = true;
            Tuples.Clear();
            if (Seen.Count >= seenLimit)
            {
                Seen.Clear();
            }
        }

        public void Overflow()
        {
            Overflowed = true;
            // Nothing downstream reads these now, and the transaction may have far more still to come.
            Tuples.Clear();
            Tuples.TrimExcess();
            Seen.Clear();
            Seen.TrimExcess();
            Representative = null!;
        }
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

/// <summary>Source-generated log messages for <see cref="DependentChangeResolver"/>.</summary>
internal static partial class DependentChangeResolverLog
{
    [LoggerMessage(Level = LogLevel.Debug, Message = "An update on dependent table {Schema}.{Table} carried no old lookup values (REPLICA IDENTITY is not FULL); a re-pointed lookup column fans out only to its new value's rows. Set REPLICA IDENTITY FULL on the table to also refresh the rows it left behind. Logged once per table.")]
    internal static partial void DependentOldLookupUnavailable(this ILogger logger, string schema, string table);
}
