using EFCore.CDC.Abstractions;
using EFCore.CDC.Model;

namespace EFCore.CDC.Internal.Backfill;

/// <summary>
/// A single backfill chunk awaiting its high watermark. While the window is open (low watermark seen,
/// high not yet), the live pipeline records the primary keys of any concurrent changes to the table so
/// that the snapshot rows they superseded can be dropped (live wins).
/// </summary>
internal sealed class PendingWindow
{
    private readonly object _gate = new();
    private IReadOnlyList<RawChange> _buffer = [];

    public required string QualifiedTable { get; init; }
    public required string LowToken { get; init; }
    public required string HighToken { get; init; }

    /// <summary>Primary keys of live changes observed for this table within the window.</summary>
    public HashSet<DocumentKey> SeenKeys { get; } = [];

    /// <summary>Set by the pipeline once the chunk has been emitted, releasing the backfill loop.</summary>
    public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The snapshot rows for this chunk (set by the backfill task before writing the high watermark).</summary>
    public IReadOnlyList<RawChange> Buffer
    {
        get { lock (_gate) return _buffer; }
        set { lock (_gate) _buffer = value; }
    }
}
