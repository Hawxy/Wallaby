using System.Runtime.CompilerServices;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using NpgsqlTypes;

namespace EFCore.CDC.Internal.Replication;

/// <summary>
/// Wraps an Npgsql <see cref="LogicalReplicationConnection"/> streaming pgoutput from a named slot, and
/// yields fully decoded <see cref="CommittedTransaction"/>s. Acknowledgement of progress is left to the
/// caller (via <see cref="AcknowledgeAsync"/>) so the slot's <c>confirmed_flush_lsn</c> only advances
/// after downstream delivery, preserving at-least-once semantics.
/// </summary>
internal sealed class LogicalReplicationStream(
    string connectionString, string slotName, string publicationName) : IAsyncDisposable
{
    private readonly LogicalReplicationConnection _connection = new(connectionString);
    private readonly PgOutputReplicationSlot _slot = new(slotName);
    // Binary mode so Npgsql decodes values to proper CLR types (e.g. DateTime, decimal) rather than text.
    private readonly PgOutputReplicationOptions _options = new(publicationName, PgOutputProtocolVersion.V1, binary: true);

    /// <summary>Stream committed transactions until cancelled.</summary>
    public async IAsyncEnumerable<CommittedTransaction> ReadAsync([EnumeratorCancellation] CancellationToken ct)
    {
        await _connection.Open(ct);
        var assembler = new TransactionAssembler();

        await foreach (var message in _connection.StartReplication(_slot, _options, ct))
        {
            // Npgsql tracks LastReceivedLsn and answers keepalives internally.
            var transaction = await assembler.ProcessAsync(message, ct);
            if (transaction is not null)
            {
                yield return transaction;
            }
        }
    }

    /// <summary>
    /// Confirm durable processing up to <paramref name="lsn"/> and flush the status to the server,
    /// allowing it to advance <c>confirmed_flush_lsn</c> and recycle WAL.
    /// </summary>
    public async Task AcknowledgeAsync(ulong lsn, CancellationToken ct)
    {
        _connection.SetReplicationStatus(new NpgsqlLogSequenceNumber(lsn));
        await _connection.SendStatusUpdate(ct);
    }

    public async ValueTask DisposeAsync() => await _connection.DisposeAsync();
}
