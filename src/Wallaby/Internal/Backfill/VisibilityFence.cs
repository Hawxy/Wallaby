using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// The opt-in snapshot visibility fence for watermark backfill: after a chunk's low watermark is
/// emitted, <see cref="WaitAsync"/> polls until no transaction in the current snapshot has already
/// committed, so a commit racing the watermark cannot be invisible to both the chunk read and the
/// window's live capture. On timeout it warns and returns; the fence narrows the race, it is not a
/// prerequisite. Requires <c>pg_xact_status</c> to be callable by Wallaby's role.
/// </summary>
internal sealed class VisibilityFence(
    TimeSpan timeout, ILogger logger,
    // Test seam: replaces the pg_xact_status probe; true means the snapshot is clean.
    Func<NpgsqlConnection, CancellationToken, Task<bool>>? probe = null)
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(10);

    // True when no xid in the current snapshot has already committed per pg_xact_status: anything still
    // invisible is then genuinely in progress, so its commit LSN lands after the low watermark and the
    // window captures it live. Immune to long-running open transactions (in progress, not committed).
    private const string ProbeSql =
        """
        SELECT NOT EXISTS (
            SELECT 1 FROM pg_snapshot_xip(pg_current_snapshot()) AS x
            WHERE pg_xact_status(x) = 'committed')
        """;

    /// <summary>The configured fence, or null when <paramref name="timeout"/> is zero (disabled).</summary>
    public static VisibilityFence? FromTimeout(TimeSpan timeout, ILogger logger)
        => timeout > TimeSpan.Zero ? new VisibilityFence(timeout, logger) : null;

    // The commit-visibility window is normally microseconds, so the first poll almost always passes.
    public async Task WaitAsync(NpgsqlConnection connection, string qualifiedTable, CancellationToken ct)
    {
        var effectiveProbe = probe
            ?? (static (conn, token) => PgExec.ScalarBoolAsync(conn, ProbeSql, token));
        var start = Stopwatch.GetTimestamp();
        while (!await effectiveProbe(connection, ct))
        {
            if (Stopwatch.GetElapsedTime(start) >= timeout)
            {
                logger.VisibilityFenceTimedOut(qualifiedTable, (long)timeout.TotalMilliseconds);
                return;
            }
            await Task.Delay(PollInterval, ct);
        }
    }
}

/// <summary>Source-generated log messages for <see cref="VisibilityFence"/>.</summary>
internal static partial class VisibilityFenceLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Visibility fence for {Table} timed out after {TimeoutMs} ms; reading the chunk without it. A commit racing the low watermark may leave a stale document until the table is next backfilled.")]
    internal static partial void VisibilityFenceTimedOut(this ILogger logger, string table, long timeoutMs);
}
