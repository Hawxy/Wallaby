---
outline: deep
---

# Custom Sinks

A sink is a destination plugin. Implement `ISink` to deliver batches of records anywhere — an HTTP API,
Kafka, another database, a cache.

## Interface

```csharp
public interface ISink
{
    string Name { get; }
    Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct);
}

public sealed record SinkBatch(string SinkName, IReadOnlyList<SinkRecord> Records);

public sealed record SinkRecord(
    string? Destination,                          // e.g. index/topic/table; null = sink default
    string DocumentId,                            // stable id for upsert/delete
    IReadOnlyDictionary<string, object?>? Document, // the field bag; null when IsDeletion
    bool IsDeletion,
    ChangeMetadata Metadata);                     // source provenance
```

Records arrive in **commit order**. Each is either an upsert of `Document` under `DocumentId`, or a
deletion of `DocumentId`.

## Returning a result

Classify the outcome so the dispatcher can react:

```csharp
return DeliveryResult.Success;                       // batch accepted
return DeliveryResult.Retry("503 from upstream");    // transient — retried with backoff
return DeliveryResult.Permanent("schema rejected");  // non-retryable — halts the pipeline
```

Retryable failures are retried with exponential backoff and jitter. A permanent failure (or exhausted
retries) halts the pipeline; the batch is retried after the leader session restarts (with its own backoff),
so a batch is never silently dropped.

## Idempotency & ordering

Delivery is **at-least-once**: the replication slot only advances after a batch is durably delivered, so a
crash can redeliver the last batch. Your sink should make delivery idempotent by supporting upsert and delete by `DocumentId`. 

Sinks should also preserve commit order - if you create batches internally, ensure you preserve it.

## One-time setup

If your sink needs setup before first delivery (create a topic, configure an index), implement
`ISinkInitializer`:

```csharp
public sealed class MySink : ISink, ISinkInitializer
{
    public Task InitializeAsync(CancellationToken ct) => /* idempotent setup */;
}
```

`InitializeAsync` runs **on the leader, once, after self-config and before streaming begins** and again
whenever a standby takes over leadership. Make it idempotent. If it throws, the leader session is retried
(the pipeline won't stream into an unconfigured sink).

## Registering

```csharp
// An instance:
cdc.AddSink(new MySink(...));

// Resolved from the container:
cdc.AddSink("my-sink", sp => new MySink(sp.GetRequiredService<HttpClient>()));
```

## The delegate sink

For in-process handlers (tests, side-effects, quick integrations), you can skip the class and use a lambda:

```csharp
cdc.AddDelegateSink("audit", async (batch, ct) =>
{
    foreach (var r in batch.Records)
    {
        if (r.IsDeletion) await store.RemoveAsync(r.DocumentId, ct);
        else              await store.UpsertAsync(r.DocumentId, r.Document!, ct);
    }
    return DeliveryResult.Success;
});
```

## Example

```csharp
public sealed class HttpSink(HttpClient http) : ISink
{
    public string Name => "http";

    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        try
        {
            foreach (var r in batch.Records)
            {
                using var resp = r.IsDeletion
                    ? await http.DeleteAsync($"/docs/{r.DocumentId}", ct)
                    : await http.PutAsJsonAsync($"/docs/{r.DocumentId}", r.Document, ct);

                if ((int)resp.StatusCode >= 500) return DeliveryResult.Retry($"upstream {(int)resp.StatusCode}");
                if (!resp.IsSuccessStatusCode)    return DeliveryResult.Permanent($"rejected {(int)resp.StatusCode}");
            }
            return DeliveryResult.Success;
        }
        catch (HttpRequestException ex) { return DeliveryResult.Retry(ex.Message, ex); }
    }
}
```
