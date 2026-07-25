---
description: "How Wallaby buffers streamed large transactions out of memory, and implementing ITransactionSpill for a custom spill backend."
---

# Transaction Spill

Wallaby uses pgoutput **protocol v2**, so a transaction larger than the server's
`logical_decoding_work_mem` (default 64 MB) is streamed before its commit. The spill buffers those
streamed changes out of process memory until the commit arrives, so a single huge transaction can't
exhaust the worker's heap. Small transactions, the overwhelming majority, never touch it.

::: tip
You likely don't need to care about this page unless you're dealing with a lot of massive transactions
:::

## Choosing a backend

```csharp
cdc.SpillToDatabase();            // default: wallaby.stream_buffer UNLOGGED table on the source DB
cdc.SpillToDisk("/var/spill");    // local temp files (path optional)
cdc.UseTransactionSpill(ctx => new S3Spill(ctx.SlotName)); // your own backend
```

- **`SpillToDatabase()`** *(default)* is disk-free and zero-config (it works wherever Wallaby
  connects), at the cost of I/O amplification on the source database during large transactions.
- **`SpillToDisk(path?)`** writes append-only files under the given path (default
  `%TEMP%/wallaby/<slot>`). It needs a writable path, so it isn't suitable for read-only
  environments.
- **`UseTransactionSpill(...)`** plugs in a custom backend. The factory runs once per leader
  session and should return a fresh instance; Wallaby disposes it when the session ends.

## Interface

```csharp
public interface ITransactionSpill : IAsyncDisposable
{
    ValueTask AppendAsync(uint xid, uint subxid, RawChange change, CancellationToken ct);
    IAsyncEnumerable<RawChange> ReadAsync(uint xid, CancellationToken ct);
    ValueTask DiscardAsync(uint xid, CancellationToken ct);
    ValueTask DiscardSubtransactionAsync(uint xid, uint subxid, CancellationToken ct);
    ValueTask ClearAsync(CancellationToken ct);
}

public readonly record struct SpillContext(
    NpgsqlDataSource DataSource,   // pooled connections to the source database
    string SlotName,               // namespace your buffered data by slot
    IServiceProvider Services);    // resolve your backend's own dependencies
```

Changes are appended per transaction (`xid`) as they stream and read back **in append order** at
the commit. An implementation owns its own serialization of `RawChange`; the abstraction deals
purely in changes.

## Implementation Guidance

- **Savepoint truncation.** `DiscardSubtransactionAsync(xid, subxid)` handles a rolled-back
  savepoint: remove the changes appended with `subxid` *and every change appended after its first
  one* (a later change can only belong to the aborted subtransaction or one nested inside it,
  which aborts with it). It must be a no-op when `subxid` never appended anything. Changes
  appended afterwards must survive and be returned by a later `ReadAsync`.
- **All-or-nothing reads.** If the backing store no longer holds everything appended for an `xid`,
  `ReadAsync` should fail rather than yield a partial buffer; a partial read that succeeded would
  be delivered and acknowledged as if complete.
- **No durability across restarts.** A streamed transaction that never commits (a crash) is
  re-streamed from the slot, so the spill need not survive a restart; `ClearAsync` drops any
  leftovers on startup.

::: warning
Do not implement an in-memory spill as you may exhaust system resources.
:::
