using System.Text;
using Npgsql.Replication.PgOutput.Messages;
using Wallaby.Abstractions;
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
/// <see cref="StreamAbortMessage"/> the spill is discarded.
/// </para>
/// Relation and truncate messages are not (yet) surfaced as changes. Generic WAL messages with the
/// <c>wallaby.watermark.*</c> prefix are buffered as <see cref="Watermark"/>s for the backfill coordinator.
/// </summary>
internal sealed class TransactionAssembler(ITransactionSpill spill, int maxBufferedChangesPerTransaction = int.MaxValue)
{
    // Non-streamed (small) transaction: changes between Begin and Commit (the common path).
    private readonly List<RawChange> _buffer = [];
    private readonly List<Watermark> _watermarks = [];

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
                return null;

            case CommitMessage commit:
                var committed = Finalize(
                    _buffer, _watermarks,
                    (ulong)commit.CommitLsn, (ulong)commit.TransactionEndLsn, commit.TransactionCommitTimestamp);
                _buffer.Clear();
                _watermarks.Clear();
                return committed;

            // ---- streamed (large) transaction boundaries (v2) ----
            case StreamStartMessage start:
                _currentStreamXid = start.TransactionXid;   // open this xid's segment; DML below spills to it
                return null;

            case StreamStopMessage:
                _currentStreamXid = null;                   // segment ended; another xid's segment, or commit/abort, follows
                return null;

            case StreamAbortMessage abort:
                await spill.DiscardAsync(abort.TransactionXid, ct);  // (sub)transaction rolled back — drop its spill
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
                };

            // ---- watermarks (only ever in tiny non-streamed transactions) ----
            case LogicalDecodingMessage msg when msg.Prefix.StartsWith(WallabySchema.WatermarkPrefix, StringComparison.Ordinal):
                _watermarks.Add(new Watermark(msg.Prefix, await ReadAsStringAsync(msg.Data, ct)));
                return null;

            // ---- DML: spill it for the open streamed xid, else buffer it for the non-streamed transaction ----
            case InsertMessage insert:
                await RouteAsync(await DecodeInsertAsync(insert, ct), ct);
                return null;

            // Derived update types first; the base UpdateMessage is abstract.
            case FullUpdateMessage update:
                await RouteAsync(await DecodeUpdateAsync(update, update.OldRow, ct), ct);
                return null;
            case IndexUpdateMessage update:
                await RouteAsync(await DecodeUpdateAsync(update, update.Key, ct), ct);
                return null;
            case DefaultUpdateMessage update:
                await RouteAsync(await DecodeUpdateAsync(update, oldRow: null, ct), ct);
                return null;

            // Derived delete types; the base DeleteMessage is abstract.
            case FullDeleteMessage delete:
                await RouteAsync(await DecodeDeleteAsync(delete, delete.OldRow, ct), ct);
                return null;
            case KeyDeleteMessage delete:
                await RouteAsync(await DecodeDeleteAsync(delete, delete.Key, ct), ct);
                return null;

            default:
                // RelationMessage, TruncateMessage, Begin/Commit-prepared, Type, etc.
                return null;
        }
    }

    private async ValueTask RouteAsync(RawChange change, CancellationToken ct)
    {
        if (_currentStreamXid is { } xid)
        {
            await spill.AppendAsync(xid, change, ct);  // streamed: out of memory, no size guard needed
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

    private static async Task<RawChange> DecodeInsertAsync(InsertMessage message, CancellationToken ct)
    {
        var newValues = await PgOutputDecoder.ReadTupleAsync(message.NewRow, ct);
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

    private static async Task<RawChange> DecodeUpdateAsync(
        UpdateMessage message, Npgsql.Replication.PgOutput.ReplicationTuple? oldRow, CancellationToken ct)
    {
        // On the wire the old tuple (when present) precedes the new tuple, so read it first.
        var oldValues = oldRow is null ? null : await PgOutputDecoder.ReadTupleAsync(oldRow, ct);
        var newValues = await PgOutputDecoder.ReadTupleAsync(message.NewRow, ct);
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

    private static async Task<RawChange> DecodeDeleteAsync(
        DeleteMessage message, Npgsql.Replication.PgOutput.ReplicationTuple oldOrKey, CancellationToken ct)
    {
        var oldValues = await PgOutputDecoder.ReadTupleAsync(oldOrKey, ct);
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

    private static CommittedTransaction Finalize(
        List<RawChange> buffer, List<Watermark> watermarks, ulong commitLsn, ulong endLsn, DateTime commitTimestamp)
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
