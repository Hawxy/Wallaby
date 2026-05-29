using EFCore.CDC.Abstractions;
using EFCore.CDC.Model;
using Npgsql.Replication.PgOutput.Messages;

namespace EFCore.CDC.Internal.Replication;

/// <summary>
/// Groups a stream of pgoutput messages into committed transactions. DML messages between
/// <see cref="BeginMessage"/> and <see cref="CommitMessage"/> are decoded and buffered; on commit the
/// buffer is stamped with the commit LSN/timestamp and returned. Relation and truncate messages are
/// not (yet) surfaced as changes.
/// </summary>
internal sealed class TransactionAssembler
{
    private readonly List<RawChange> _buffer = [];

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
            changes[i] = _buffer[i] with
            {
                CommitLsn = commitLsn,
                CommitTimestamp = timestamp,
                CommitIdx = i,
            };
        }

        return new CommittedTransaction
        {
            CommitLsn = commitLsn,
            EndLsn = endLsn,
            CommitTimestamp = timestamp,
            Changes = changes,
        };
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
