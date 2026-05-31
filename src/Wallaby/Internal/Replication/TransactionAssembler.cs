using System.Text;
using Npgsql.Replication.PgOutput.Messages;
using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Groups a stream of pgoutput messages into committed transactions. DML messages between
/// <see cref="BeginMessage"/> and <see cref="CommitMessage"/> are decoded and buffered; on commit the
/// buffer is stamped with the commit LSN/timestamp and returned. Relation and truncate messages are
/// not (yet) surfaced as changes. Generic WAL messages with the <c>cdc.watermark.*</c> prefix are
/// buffered as <see cref="Watermark"/>s for the backfill coordinator.
/// </summary>
internal sealed class TransactionAssembler
{
    private readonly List<RawChange> _buffer = [];
    private readonly List<Watermark> _watermarks = [];

    /// <summary>
    /// Process one message. Returns a committed transaction when a <see cref="CommitMessage"/> is seen,
    /// otherwise null. Must be awaited fully before the next message is read (recycled-message rule).
    /// </summary>
    public async Task<CommittedTransaction?> ProcessAsync(PgOutputReplicationMessage message, CancellationToken ct)
    {
        switch (message)
        {
            case BeginMessage:
                _buffer.Clear();
                _watermarks.Clear();
                return null;

            case LogicalDecodingMessage msg when msg.Prefix.StartsWith(CdcSchema.WatermarkPrefix, StringComparison.Ordinal):
                _watermarks.Add(new Watermark(msg.Prefix, await ReadAsStringAsync(msg.Data, ct)));
                return null;

            case InsertMessage insert:
                _buffer.Add(await DecodeInsertAsync(insert, ct));
                return null;

            // Derived update types first; the base UpdateMessage is abstract.
            case FullUpdateMessage update:
                _buffer.Add(await DecodeUpdateAsync(update, update.OldRow, ct));
                return null;
            case IndexUpdateMessage update:
                _buffer.Add(await DecodeUpdateAsync(update, update.Key, ct));
                return null;
            case DefaultUpdateMessage update:
                _buffer.Add(await DecodeUpdateAsync(update, oldRow: null, ct));
                return null;

            // Derived delete types; the base DeleteMessage is abstract.
            case FullDeleteMessage delete:
                _buffer.Add(await DecodeDeleteAsync(delete, delete.OldRow, ct));
                return null;
            case KeyDeleteMessage delete:
                _buffer.Add(await DecodeDeleteAsync(delete, delete.Key, ct));
                return null;

            case CommitMessage commit:
                var transaction = Finalize(
                    (ulong)commit.CommitLsn, (ulong)commit.TransactionEndLsn, commit.TransactionCommitTimestamp);
                _buffer.Clear();
                _watermarks.Clear();
                return transaction;

            default:
                // RelationMessage, TruncateMessage, Begin/Commit-prepared, Stream*, Type, etc.
                return null;
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

    private CommittedTransaction Finalize(ulong commitLsn, ulong endLsn, DateTime commitTimestamp)
    {
        var timestamp = NormalizeTimestamp(commitTimestamp);
        var changes = new RawChange[_buffer.Count];
        for (var i = 0; i < _buffer.Count; i++)
        {
            var change = _buffer[i];
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
            Watermarks = _watermarks.Count == 0 ? Array.Empty<Watermark>() : _watermarks.ToArray(),
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
