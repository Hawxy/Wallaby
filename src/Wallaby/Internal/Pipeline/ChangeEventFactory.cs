using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NpgsqlTypes;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Composes a <see cref="ChangeEvent"/> from a decoded <see cref="RawChange"/> by materializing it and
/// attaching commit/source metadata. Returns null when the change's table is not part of the model.
/// <para>
/// A change whose unchanged TOASTed value was not on the wire (<see cref="UnavailableValueException"/>)
/// is healed by re-reading the row when a reselector is supplied: a live row materializes from current
/// row state, and a vanished row's change is dropped (its delete follows later in the stream). Any other
/// materialization <em>failure</em> (e.g. a bad value/conversion or a missing key) is a poison change that
/// always halts the pipeline; it is rethrown annotated with the change's table and commit position.
/// </para>
/// </summary>
internal sealed class ChangeEventFactory(
    IRowMaterializer materializer,
    IRowReselector? reselector = null,
    ILogger? logger = null,
    WallabyInstrumentation? instrumentation = null)
{
    // A table healing steadily would otherwise warn per change; the first heal per table logs
    // immediately, later ones roll up into at most one line per table per interval. Only the pipeline
    // loop calls CreateAsync, so the dictionary needs no synchronization.
    private static readonly TimeSpan HealLogInterval = TimeSpan.FromSeconds(30);

    private readonly ILogger _logger = logger ?? NullLogger.Instance;
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;
    private readonly Dictionary<string, TableHealLog> _healLogs = new(StringComparer.Ordinal);

    private sealed class TableHealLog
    {
        public long Suppressed;
        public long LastLoggedAt;
    }

    public async ValueTask<ChangeEvent?> CreateAsync(RawChange change, CancellationToken ct)
    {
        try
        {
            return Compose(change, Materialize(change));
        }
        catch (UnavailableValueException ex)
        {
            // Backfill/fan-out rows are synthesized from SELECTs and cannot omit values; a typed
            // exception there indicates a bug and halts like any other materialization failure.
            if (reselector is null || change.Action is not (ChangeAction.Insert or ChangeAction.Update))
            {
                throw Annotated(change, ex);
            }

            RawChange? healed;
            try
            {
                healed = await reselector.ReselectAsync(change, ct);
            }
            catch (Exception reselectEx) when (reselectEx is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"Materialization failed for a {change.Action} on {change.QualifiedName} " +
                    $"(commit {Lsn(change)}, change #{change.CommitIdx}): the reselect for unavailable " +
                    $"column '{ex.ColumnName}' failed: {reselectEx.Message}", reselectEx);
            }

            if (healed is null)
            {
                _logger.DroppedVanishedRow(change.CommitIdx, Lsn(change), change.QualifiedName, ex.ColumnName);
                _instr.RecordReselect(change.QualifiedName, WallabyInstrumentation.ReselectRowGone);
                return null;
            }

            LogHealed(change, ex.ColumnName);
            _instr.RecordReselect(change.QualifiedName, WallabyInstrumentation.ReselectHealed);
            return Compose(healed, Materialize(healed));
        }
    }

    private MaterializedRow? Materialize(RawChange change)
    {
        try
        {
            return materializer.TryMaterialize(change, out var row) ? row : null;
        }
        catch (UnavailableValueException)
        {
            throw; // recovered (or rethrown annotated) by CreateAsync
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw Annotated(change, ex);
        }
    }

    private static ChangeEvent? Compose(RawChange change, MaterializedRow? row)
    {
        if (row is null)
        {
            return null; // table not part of the model; benign skip
        }

        var metadata = new ChangeMetadata(
            change.Schema,
            change.TableName,
            row.Action,
            change.CommitTimestamp,
            change.CommitLsn,
            change.CommitIdx,
            IsBackfill: change.Action == ChangeAction.Read,
            change.BackfillRunId);

        return new ChangeEvent(
            row.Action,
            metadata,
            row.Entity,
            row.Record,
            row.Changes,
            row.PrimaryKey)
        {
            EntityClrType = row.EntityClrType,
        };
    }

    private void LogHealed(RawChange change, string column)
    {
        if (!_healLogs.TryGetValue(change.QualifiedName, out var log))
        {
            _healLogs[change.QualifiedName] = new TableHealLog { LastLoggedAt = Stopwatch.GetTimestamp() };
            _logger.RecoveredUnavailableValue(change.CommitIdx, Lsn(change), change.QualifiedName, column);
            return;
        }

        log.Suppressed++;
        if (Stopwatch.GetElapsedTime(log.LastLoggedAt) < HealLogInterval)
        {
            return;
        }

        _logger.RecoveredUnavailableValuesRollup(log.Suppressed, change.QualifiedName, column, Lsn(change));
        log.Suppressed = 0;
        log.LastLoggedAt = Stopwatch.GetTimestamp();
    }

    private static InvalidOperationException Annotated(RawChange change, Exception ex)
        => new(
            $"Materialization failed for a {change.Action} on {change.QualifiedName} " +
            $"(commit {Lsn(change)}, change #{change.CommitIdx}): {ex.Message}", ex);

    private static NpgsqlLogSequenceNumber Lsn(RawChange change) => new(change.CommitLsn);
}

internal static partial class ChangeEventFactoryLog
{
    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Change #{CommitIdx} of commit {CommitLsn} on {Table} omitted column '{Column}' (an unchanged " +
                  "TOASTed value under REPLICA IDENTITY DEFAULT); healed by re-reading current row state. " +
                  "Set REPLICA IDENTITY FULL to stop paying a re-read per change; both providers offer a " +
                  "managed way to apply it. See " +
                  "https://wallabycdc.net/how-it-works#unavailable-value-self-healing-reselect")]
    public static partial void RecoveredUnavailableValue(
        this ILogger logger, int commitIdx, NpgsqlLogSequenceNumber commitLsn, string table, string column);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Healed {Count} more change(s) on {Table} by re-reading current row state (latest: column " +
                  "'{Column}', commit {CommitLsn}); still paying a re-read per change. See " +
                  "https://wallabycdc.net/how-it-works#unavailable-value-self-healing-reselect")]
    public static partial void RecoveredUnavailableValuesRollup(
        this ILogger logger, long count, string table, string column, NpgsqlLogSequenceNumber commitLsn);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Change #{CommitIdx} of commit {CommitLsn} on {Table} omitted column '{Column}' and the row " +
                  "no longer exists; dropped the change (its delete follows later in the stream).")]
    public static partial void DroppedVanishedRow(
        this ILogger logger, int commitIdx, NpgsqlLogSequenceNumber commitLsn, string table, string column);
}
