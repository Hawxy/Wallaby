using EFCore.CDC.Abstractions;

namespace EFCore.CDC.TestInfrastructure;

/// <summary>
/// An <see cref="ISink"/> that records every delivered record for assertions. Thread-safe; supports
/// filtering by source table and clearing between phases (e.g. before/after a restart).
/// </summary>
public sealed class CaptureSink(string name = "capture") : ISink
{
    private readonly List<SinkRecord> _records = [];

    public string Name { get; } = name;

    public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        lock (_records)
        {
            _records.AddRange(batch.Records);
        }
        return Task.FromResult(DeliveryResult.Success);
    }

    /// <summary>A snapshot of all records delivered so far.</summary>
    public IReadOnlyList<SinkRecord> Records
    {
        get { lock (_records) return _records.ToArray(); }
    }

    /// <summary>Records whose source table matches <paramref name="tableName"/>.</summary>
    public IEnumerable<SinkRecord> For(string tableName) => Records.Where(r => r.Metadata.TableName == tableName);

    /// <summary>Discard recorded records (e.g. to isolate a second run from a first).</summary>
    public void Clear()
    {
        lock (_records) _records.Clear();
    }
}
