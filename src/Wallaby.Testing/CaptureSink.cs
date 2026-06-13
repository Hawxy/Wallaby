using Wallaby.Abstractions;

namespace Wallaby.Testing;

/// <summary>
/// An <see cref="ISink"/> that records every delivered record for assertions. Thread-safe; supports
/// waiting for expected records to arrive (Wallaby delivery is asynchronous), filtering by source table,
/// and clearing between test phases.
/// </summary>
/// <param name="name">
/// The sink's own name. Note that batch routing is keyed by the <em>registration</em> name (the name
/// passed to <c>WallabyBuilder.AddSink</c> or <see cref="WallabyTestingServiceCollectionExtensions.ReplaceWallabySink"/>),
/// so this value is informational unless the sink is registered via <c>AddSink(ISink)</c>.
/// </param>
public sealed class CaptureSink(string name = "capture") : ISink
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(50);

    private readonly List<SinkRecord> _records = [];

    /// <inheritdoc />
    public string Name { get; } = name;

    /// <inheritdoc />
    public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        lock (_records)
        {
            _records.AddRange(batch.Records);
        }
        return Task.FromResult(DeliveryResult.Success);
    }

    /// <summary>A snapshot of all records delivered so far, in delivery order.</summary>
    public IReadOnlyList<SinkRecord> Records
    {
        get { lock (_records) return _records.ToArray(); }
    }

    /// <summary>Records whose source table matches <paramref name="tableName"/>.</summary>
    public IEnumerable<SinkRecord> For(string tableName) => Records.Where(r => r.Metadata.TableName == tableName);

    /// <summary>Discard recorded records (e.g. to isolate one test from the previous one).</summary>
    public void Clear()
    {
        lock (_records) _records.Clear();
    }

    /// <summary>
    /// Poll until <paramref name="predicate"/> passes over the records delivered so far, then return that
    /// snapshot. Use this instead of asserting immediately — Wallaby delivery is asynchronous, so records arrive
    /// some time after the triggering commit.
    /// </summary>
    /// <param name="predicate">Evaluated against a snapshot of all recorded records on each poll.</param>
    /// <param name="timeout">How long to wait before giving up; defaults to 30 seconds.</param>
    /// <param name="ct">Cancels the wait.</param>
    /// <exception cref="TimeoutException">
    /// The predicate did not pass in time. The message lists the records received so far for diagnosis.
    /// </exception>
    public async Task<IReadOnlyList<SinkRecord>> WaitForAsync(
        Func<IReadOnlyList<SinkRecord>, bool> predicate, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var effectiveTimeout = timeout ?? DefaultTimeout;
        var deadline = DateTime.UtcNow + effectiveTimeout;
        while (true)
        {
            var snapshot = Records;
            if (predicate(snapshot))
            {
                return snapshot;
            }
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"CaptureSink '{Name}' did not satisfy the predicate within {effectiveTimeout}. " +
                    $"Received {snapshot.Count} record(s):{Environment.NewLine}{Describe(snapshot)}");
            }
            await Task.Delay(PollInterval, ct);
        }
    }

    /// <summary>
    /// Wait until every id in <paramref name="documentIds"/> has at least one delivered record, then return
    /// a snapshot of all records.
    /// </summary>
    /// <param name="documentIds">The <see cref="SinkRecord.DocumentId"/> values to wait for.</param>
    /// <param name="timeout">How long to wait before giving up; defaults to 30 seconds.</param>
    /// <param name="ct">Cancels the wait.</param>
    /// <exception cref="TimeoutException">One or more ids never arrived in time.</exception>
    public Task<IReadOnlyList<SinkRecord>> WaitForDocumentsAsync(
        IEnumerable<string> documentIds, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var ids = documentIds.ToArray();
        return WaitForAsync(records => ids.All(id => records.Any(r => r.DocumentId == id)), timeout, ct);
    }

    /// <summary>
    /// The last delivered record per <see cref="SinkRecord.DocumentId"/> — the "current state" view of the
    /// destination after applying all deliveries in order (an upsert followed by a deletion leaves the
    /// deletion).
    /// </summary>
    /// <param name="destination">When set, only records routed to this destination are considered.</param>
    public IReadOnlyDictionary<string, SinkRecord> LatestByDocumentId(string? destination = null)
    {
        var result = new Dictionary<string, SinkRecord>(StringComparer.Ordinal);
        foreach (var record in Records)
        {
            if (destination is not null && record.Destination != destination)
            {
                continue;
            }
            result[record.DocumentId] = record;
        }
        return result;
    }

    private static string Describe(IReadOnlyList<SinkRecord> records)
        => records.Count == 0
            ? "  (none)"
            : string.Join(Environment.NewLine, records.Select(r =>
                $"  {r.Metadata.QualifiedTableName} {(r.IsDeletion ? "delete" : "upsert")} id={r.DocumentId} dest={r.Destination ?? "(default)"}"));
}
