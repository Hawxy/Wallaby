using System.Text;
using Npgsql.Replication.PgOutput.Messages;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Groups a stream of pgoutput messages into committed transactions.
/// <para>
/// A normal (small) transaction arrives as <see cref="BeginMessage"/> … DML … <see cref="CommitMessage"/> and
/// is buffered in memory, then stamped with the commit LSN/timestamp and returned on commit.
/// </para>
/// <para>
/// Under pgoutput <b>v2 streaming</b>, a transaction larger than the server's <c>logical_decoding_work_mem</c>
/// is sent <em>before</em> commit as one or more segments (<see cref="StreamStartMessage"/> … DML …
/// <see cref="StreamStopMessage"/>), possibly interleaved with other transactions. Its changes are
/// <b>spilled</b> (via <see cref="ITransactionSpill"/>) rather than buffered in memory, so a single huge
/// transaction can't exhaust the heap; on <see cref="StreamCommitMessage"/> a spill-backed
/// <see cref="CommittedTransaction"/> is returned (the pipeline reads the changes back in pages), and on
/// <see cref="StreamAbortMessage"/> the spill is discarded: wholly for a transaction abort, or truncated from
/// the subtransaction's first change for a rolled-back savepoint.
/// </para>
/// Relation messages are not surfaced as changes. Truncates of captured tables are surfaced as
/// <see cref="CommittedTransaction.TruncatedTables"/> (the pipeline warns; nothing reaches sinks).
/// Generic WAL messages with the <c>wallaby.watermark.*</c> prefix are buffered as
/// <see cref="Watermark"/>s for the backfill coordinator.
/// </summary>
internal sealed class TransactionAssembler(
    ITransactionSpill spill, int maxBufferedChangesPerTransaction = int.MaxValue, WallabyModel? model = null,
    string slotName = "", WallabyInstrumentation? instrumentation = null)
{
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;

    // Non-streamed (small) transaction: changes between Begin and Commit (the common path).
    private readonly List<RawChange> _buffer = [];
    private readonly List<Watermark> _watermarks = [];

    // Changes appended to the spill per open streamed xid (savepoint rollbacks are not subtracted);
    // stamped onto the committed transaction so its span can carry the spill volume.
    private readonly Dictionary<uint, int> _spilledByXid = [];

    // True when the open non-streamed transaction carried a wallaby.heartbeat message.
    private bool _sawHeartbeat;

    // Qualified names of captured tables truncated in the open non-streamed transaction.
    private readonly List<string> _truncatedTables = [];

    // Same, keyed by streamed xid; lazy because truncates are rare.
    private Dictionary<uint, List<string>>? _streamedTruncates;

    // Per-relation column read modes, aligned to the relation's column order; a null value means every
    // column reads with ColumnReadMode.Default (also cached, so unflagged and unmodeled tables pay the
    // model lookup once). Invalidated when the server re-sends a RelationMessage after a schema change.
    private readonly Dictionary<uint, ColumnReadMode[]?> _readModesByRelation = [];

    // The xid of the streamed segment currently open (between StreamStart and StreamStop); null otherwise.
    private uint? _currentStreamXid;

    /// <summary>
    /// Process one message. Returns a committed transaction when a <see cref="CommitMessage"/> or
    /// <see cref="StreamCommitMessage"/> is seen, otherwise null. Must be awaited fully before the next message
    /// is read (recycled-message rule).
    /// </summary>
    public async Task<CommittedTransaction?> ProcessAsync(PgOutputReplicationMessage message, CancellationToken ct)
    {
        switch (message)
        {
            // ---- non-streamed transaction boundaries ----
            case BeginMessage:
                _buffer.Clear();
                _watermarks.Clear();
                _sawHeartbeat = false;
                _truncatedTables.Clear();
                return null;

            case CommitMessage commit:
                var committed = Finalize(
                    _buffer, _watermarks, _sawHeartbeat, _truncatedTables,
                    (ulong)commit.CommitLsn, (ulong)commit.TransactionEndLsn, commit.TransactionCommitTimestamp);
                _buffer.Clear();
                _watermarks.Clear();
                _sawHeartbeat = false;
                _truncatedTables.Clear();
                return committed;

            // ---- streamed (large) transaction boundaries (v2) ----
            case StreamStartMessage start:
                _currentStreamXid = start.TransactionXid;   // open this xid's segment; DML below spills to it
                return null;

            case StreamStopMessage:
                _currentStreamXid = null;                   // segment ended; another xid's segment, or commit/abort, follows
                return null;

            case StreamAbortMessage abort when abort.SubtransactionXid == abort.TransactionXid:
                await spill.DiscardAsync(abort.TransactionXid, ct);  // whole transaction rolled back; drop its spill
                _streamedTruncates?.Remove(abort.TransactionXid);
                _spilledByXid.Remove(abort.TransactionXid);
                return null;

            case StreamAbortMessage abort:
                // A rolled-back savepoint: truncate just that subtransaction's changes; the rest of the
                // transaction streams on and may still commit.
                await spill.DiscardSubtransactionAsync(abort.TransactionXid, abort.SubtransactionXid, ct);
                return null;

            case StreamCommitMessage streamCommit:
                _currentStreamXid = null;
                return new CommittedTransaction
                {
                    CommitLsn = (ulong)streamCommit.CommitLsn,
                    EndLsn = (ulong)streamCommit.TransactionEndLsn,
                    CommitTimestamp = NormalizeTimestamp(streamCommit.TransactionCommitTimestamp),
                    Changes = [],
                    IsStreamed = true,
                    StreamXid = streamCommit.TransactionXid,
                    Spill = spill,
                    SpilledChanges = _spilledByXid.Remove(streamCommit.TransactionXid, out var spilled) ? spilled : 0,
                    TruncatedTables = _streamedTruncates is not null
                        && _streamedTruncates.Remove(streamCommit.TransactionXid, out var truncated)
                        ? truncated : Array.Empty<string>(),
                };

            // ---- watermarks (only ever in tiny non-streamed transactions) ----
            case LogicalDecodingMessage msg when msg.Prefix.StartsWith(WallabySchema.WatermarkPrefix, StringComparison.Ordinal):
                _watermarks.Add(new Watermark(msg.Prefix, await ReadAsStringAsync(msg.Data, ct)));
                return null;

            // ---- idle-slot heartbeats (only ever in tiny non-streamed transactions) ----
            case LogicalDecodingMessage msg when msg.Prefix == WallabySchema.HeartbeatPrefix:
                _ = await ReadAsStringAsync(msg.Data, ct);  // recycled-message rule: drain even an empty payload
                _sawHeartbeat = true;
                return null;

            // ---- DML: spill it for the open streamed xid, else buffer it for the non-streamed transaction ----
            case InsertMessage insert:
                await RouteAsync(await DecodeInsertAsync(insert, ct), insert.TransactionXid, ct);
                return null;

            // Derived update types first; the base UpdateMessage is abstract.
            case FullUpdateMessage update:
                await RouteAsync(await DecodeUpdateAsync(update, update.OldRow, ct), update.TransactionXid, ct);
                return null;
            case IndexUpdateMessage update:
                await RouteAsync(await DecodeUpdateAsync(update, update.Key, ct), update.TransactionXid, ct);
                return null;
            case DefaultUpdateMessage update:
                await RouteAsync(await DecodeUpdateAsync(update, oldRow: null, ct), update.TransactionXid, ct);
                return null;

            // Derived delete types; the base DeleteMessage is abstract.
            case FullDeleteMessage delete:
                await RouteAsync(await DecodeDeleteAsync(delete, delete.OldRow, ct), delete.TransactionXid, ct);
                return null;
            case KeyDeleteMessage delete:
                await RouteAsync(await DecodeDeleteAsync(delete, delete.Key, ct), delete.TransactionXid, ct);
                return null;

            case RelationMessage relation:
                // Sent before a relation's first DML and re-sent after a schema change; drop any cached
                // read-mode plan so the next DML rebuilds it against the new column layout.
                _readModesByRelation.Remove(relation.RelationId);
                return null;

            // ---- truncate: never propagated as a change; captured tables are noted so the pipeline can warn ----
            case TruncateMessage truncate:
                // Recycled-message rule: resolve namespace/name now; the RelationMessages are only valid here.
                foreach (var relation in truncate.Relations)
                {
                    if (model?.FindByRelation(relation.Namespace, relation.RelationName) is { } table)
                    {
                        var target = _currentStreamXid is { } streamXid
                            ? StreamedTruncatesFor(streamXid)
                            : _truncatedTables;
                        if (!target.Contains(table.QualifiedName))
                        {
                            target.Add(table.QualifiedName);
                        }
                    }
                }
                return null;

            default:
                // Begin/Commit-prepared, Type, etc.
                return null;
        }
    }

    // messageXid is the streamed DML message's own xid: the subtransaction's when made inside a
    // savepoint, the toplevel's otherwise (and null on non-streamed messages).
    private async ValueTask RouteAsync(RawChange change, uint? messageXid, CancellationToken ct)
    {
        if (_currentStreamXid is { } xid)
        {
            await spill.AppendAsync(xid, messageXid ?? xid, change, ct);  // streamed: out of memory, no size guard needed
            _spilledByXid[xid] = _spilledByXid.GetValueOrDefault(xid) + 1;
            _instr.RecordSpilledChange(slotName);
            return;
        }

        _buffer.Add(change);
        if (_buffer.Count > maxBufferedChangesPerTransaction)
        {
            throw new InvalidOperationException(
                $"A non-streamed transaction buffered more than MaxBufferedChangesPerTransaction " +
                $"({maxBufferedChangesPerTransaction}) changes. Increase WallabyOptions.Advanced.MaxBufferedChangesPerTransaction, " +
                "or lower the server's logical_decoding_work_mem so large transactions stream and spill.");
        }
    }

    // Resolved once per DML message and applied to the new AND old tuples (the unchanged-TOAST
    // fallback reads the body from the old tuple). Built lazily on first DML rather than on the
    // RelationMessage itself, because Npgsql recycles the message object; message.Relation on a DML
    // message is valid within its own iteration.
    private ColumnReadMode[]? ReadModesFor(RelationMessage relation)
    {
        if (model is null)
        {
            return null;
        }
        if (_readModesByRelation.TryGetValue(relation.RelationId, out var modes))
        {
            return modes;
        }
        return _readModesByRelation[relation.RelationId] = BuildReadModes(relation);
    }

    private ColumnReadMode[]? BuildReadModes(RelationMessage relation)
    {
        if (model!.FindByRelation(relation.Namespace, relation.RelationName) is not { } table)
        {
            return null;
        }

        ColumnReadMode[]? modes = null;
        for (var i = 0; i < relation.Columns.Count; i++)
        {
            var name = relation.Columns[i].ColumnName;
            foreach (var column in table.Columns)
            {
                if (column.ReadMode != ColumnReadMode.Default && column.ColumnName == name)
                {
                    (modes ??= new ColumnReadMode[relation.Columns.Count])[i] = column.ReadMode;
                    break;
                }
            }
        }
        return modes;
    }

    private async ValueTask<RawChange> DecodeInsertAsync(InsertMessage message, CancellationToken ct)
    {
        var readModes = ReadModesFor(message.Relation);
        var newValues = await PgOutputDecoder.ReadTupleAsync(message.NewRow, readModes, message.Relation, message.WalStart, ct);
        return new RawChange
        {
            RelationId = message.Relation.RelationId,
            Schema = message.Relation.Namespace,
            TableName = message.Relation.RelationName,
            Action = ChangeAction.Insert,
            NewValues = newValues,
            OldValues = null,
        };
    }

    private async ValueTask<RawChange> DecodeUpdateAsync(
        UpdateMessage message, Npgsql.Replication.PgOutput.ReplicationTuple? oldRow, CancellationToken ct)
    {
        var readModes = ReadModesFor(message.Relation);
        // On the wire the old tuple (when present) precedes the new tuple, so read it first.
        var oldValues = oldRow is null
            ? null
            : await PgOutputDecoder.ReadTupleAsync(oldRow, readModes, message.Relation, message.WalStart, ct);
        var newValues = await PgOutputDecoder.ReadTupleAsync(message.NewRow, readModes, message.Relation, message.WalStart, ct);
        return new RawChange
        {
            RelationId = message.Relation.RelationId,
            Schema = message.Relation.Namespace,
            TableName = message.Relation.RelationName,
            Action = ChangeAction.Update,
            NewValues = newValues,
            OldValues = oldValues,
        };
    }

    private async ValueTask<RawChange> DecodeDeleteAsync(
        DeleteMessage message, Npgsql.Replication.PgOutput.ReplicationTuple oldOrKey, CancellationToken ct)
    {
        var oldValues = await PgOutputDecoder.ReadTupleAsync(
            oldOrKey, ReadModesFor(message.Relation), message.Relation, message.WalStart, ct);
        return new RawChange
        {
            RelationId = message.Relation.RelationId,
            Schema = message.Relation.Namespace,
            TableName = message.Relation.RelationName,
            Action = ChangeAction.Delete,
            NewValues = [],
            OldValues = oldValues,
        };
    }

    // A truncate rolled back via savepoint may still be recorded here; accepted, the result is only a warning.
    private List<string> StreamedTruncatesFor(uint xid)
    {
        _streamedTruncates ??= [];
        if (!_streamedTruncates.TryGetValue(xid, out var list))
        {
            _streamedTruncates[xid] = list = [];
        }
        return list;
    }

    private static CommittedTransaction Finalize(
        List<RawChange> buffer, List<Watermark> watermarks, bool containsHeartbeat, List<string> truncatedTables,
        ulong commitLsn, ulong endLsn, DateTime commitTimestamp)
    {
        var timestamp = NormalizeTimestamp(commitTimestamp);
        var changes = new RawChange[buffer.Count];
        for (var i = 0; i < buffer.Count; i++)
        {
            var change = buffer[i];
            change.CommitLsn = commitLsn;
            change.CommitTimestamp = timestamp;
            change.CommitIdx = i;
            changes[i] = change;
        }

        return new CommittedTransaction
        {
            CommitLsn = commitLsn,
            EndLsn = endLsn,
            CommitTimestamp = timestamp,
            Changes = changes,
            Watermarks = watermarks.Count == 0 ? Array.Empty<Watermark>() : watermarks.ToArray(),
            ContainsHeartbeat = containsHeartbeat,
            TruncatedTables = truncatedTables.Count == 0 ? Array.Empty<string>() : truncatedTables.ToArray(),
        };
    }

    // pgoutput recycles the LogicalDecodingMessage (its Data stream is backed by the connection buffer),
    // so we MUST fully consume the stream in the same loop iteration before reading the next message.
    private static async Task<string> ReadAsStringAsync(Stream data, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        await data.CopyToAsync(buffer, ct);
        return Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
    }

    private static DateTimeOffset? NormalizeTimestamp(DateTime value)
    {
        if (value == default) return null;
        return value.Kind switch
        {
            DateTimeKind.Utc => new DateTimeOffset(value),
            DateTimeKind.Local => new DateTimeOffset(value.ToUniversalTime()),
            _ => new DateTimeOffset(DateTime.SpecifyKind(value, DateTimeKind.Utc)),
        };
    }
}
