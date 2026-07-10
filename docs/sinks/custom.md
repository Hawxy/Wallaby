---
description: "Implementing ISink to deliver change batches to any destination, and the delivery contract a sink must honor."
---

# Custom Sinks

A sink is a destination plugin. Implement `ISink` to deliver batches of records anywhere - another
database, a message broker, a cache.

::: tip
We're always looking for new sink contributions. Feel free to open a pull request for review.
:::

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
return DeliveryResult.Retry("503 from upstream");    // transient - retried with backoff
return DeliveryResult.Permanent("schema rejected");  // non-retryable - halts the pipeline
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

## Cleanup

Registering a sink hands its lifetime to Wallaby. If your sink
holds resources (a client, a connection pool, a producer), implement `IAsyncDisposable` (or
`IDisposable`):

```csharp
public sealed class MySink : ISink, IAsyncDisposable
{
    public ValueTask DisposeAsync() => _client.DisposeAsync();
}
```

Disposal runs **once, at host shutdown**, after streaming has stopped. 
A sink implementing both interfaces is disposed via `DisposeAsync` only, and a throwing
dispose is logged without disrupting the rest of shutdown.

## Registering

```csharp
// An instance:
cdc.AddSink(new MySink(...))
   .WithMappings(sink => sink.Map<Product>().UsingTransform(/* ... */));

// Resolved from the container:
cdc.AddSink("my-sink", sp => new MySink(sp.GetRequiredService<HttpClient>()))
   .WithMappings(sink => sink.Map<Product>().UsingTransform(/* ... */));
```

`AddSink` returns a sink-scoped builder: declare the entities the sink receives in `WithMappings(...)`,
or continue the chain via its `Wallaby` property for a sink registered without mappings.

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

::: tip
A production-ready HTTP sink ships as [`Wallaby.Sinks.Http`](/sinks/http) with batched envelopes, HMAC
signing, and `IHttpClientFactory` integration. The example below is a simple example.
:::

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
