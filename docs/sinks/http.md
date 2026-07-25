---
description: "POSTing change batches to any HTTP endpoint as a JSON envelope, with named-client auth, HMAC signing, and idempotency keys."
---

# HTTP Sink

The `Wallaby.Sinks.Http` package POSTs batches of changes to any HTTP endpoint as a JSON envelope of
upsert/delete records. Retries, ordering, and at-least-once delivery are handled by the pipeline; your receiver just applies
records idempotently.

## Install

```bash
dotnet add package Wallaby.Sinks.Http
```

## Register

```csharp
builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>()
       .UseConnectionString(conn)
       .AddHttpSink("webhook", o =>
       {
           o.Endpoint = "https://api.example.com/wallaby";
           o.SigningSecret = secret; // optional HMAC body signing
       })
       .WithMappings(sink => sink
           .Map<Product>()
           .ToDestination("products")
           .UsingTransform(/* ... */));
});
```

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `Endpoint` | *(required)* | Absolute URL every envelope is POSTed to. |
| `HttpClientName` | `wallaby.sinks.http.<name>` | The `IHttpClientFactory` named client used for delivery. |
| `SigningSecret` | `null` | Enables HMAC-SHA256 [body signing](#verifying-signatures). |
| `Compression` | `None` | [Request-body compression](#compression): `Gzip` or `Brotli`. |
| `Annotations` | `null` | Static key/values echoed at the top of every envelope. |
| `MaxRecordsPerRequest` | `500` | Larger batches are split into sequential requests (commit order preserved). |
| `TimeoutMs` | `30000` | Per-request timeout; composes with any timeout on the named client. |
| `SerializerOptions` | `null` | Serializer for non-scalar document values - see [NativeAOT](#nativeaot). |

## Authentication

Configure auth via the `IHttpClientFactory` **named client**. The sink adds nothing to the request but
the body and its signature:

```csharp
builder.Services.AddHttpClient(HttpSink.ClientNameFor("webhook")) 
    .ConfigureHttpClient(c => c.DefaultRequestHeaders.Add("X-Api-Key", apiKey))
    .AddHttpMessageHandler<OAuthTokenHandler>(); // your DelegatingHandler
```

## The envelope

Each request is a JSON envelope; `records` preserves commit order:

```json
{
  "sink": "webhook",
  "sentAt": "2026-07-06T03:12:45.123Z",
  "records": [
    {
      "operation": "upsert",
      "id": "42",
      "idempotencyKey": "27271208:0:products:42",
      "destination": "products",
      "document": { "name": "Kangaroo plush", "price": 19.95 },
      "metadata": {
        "schema": "public",
        "table": "products",
        "action": "insert",
        "commitLsn": "27271208",
        "commitIdx": 0,
        "commitTimestamp": "2026-07-06T03:12:45.100Z",
        "isBackfill": false
      }
    },
    {
      "operation": "delete",
      "id": "43",
      "idempotencyKey": "27271208:1:products:43",
      "destination": "products",
      "metadata": {
        "schema": "public", "table": "products", "action": "delete",
        "commitLsn": "27271208", "commitIdx": 1, "isBackfill": false
      }
    }
  ]
}
```

- `operation` is `upsert` (apply `document` under `id`) or `delete` (remove `id`); a delete carries no `document`.
- `idempotencyKey` is an opaque string unique to each delivered change - store it to
  [reject redelivered duplicates](#delivery-semantics). A backfill row's key embeds a per-run token
  (echoed as `metadata.backfillRunId`): stable within one run, **new for every run**, so a re-backfill
  (e.g. a `WithBackfillVersion` bump) is never suppressed by stored keys.
- `destination` is the mapping's `ToDestination(...)` value (or a [`ScopedDestination`](/providers/entity-framework-core/multi-tenancy) result); `null` when the mapping declares none.
- `metadata.action` is what the change meant in the source model: `insert`, `update`, `delete`, or `read`
  (a backfill row). Providers may substitute meaning - e.g. Marten surfaces a soft-delete `UPDATE` as
  `delete` - so it can differ from the raw WAL operation.
- `commitLsn` is a string - the value can exceed the safe-integer range of JavaScript consumers. Backfill
  records have `commitLsn: "0"` and omit `commitTimestamp`.
- With `Annotations` configured, the envelope carries an `annotations` object with those key/values
  alongside `sink` and `sentAt`.

## Delivery semantics

Delivery is **at-least-once**: a crash can redeliver a batch your receiver already processed, so apply
records idempotently - upsert by `id`, delete by `id`, and treat a delete for an unknown id as success.
If your receiver has side effects beyond state (e.g. sends an email per record), store each record's
`idempotencyKey` and skip keys you have seen; `(commitLsn, commitIdx)` orders live changes. Two caveats
for backfill rows: a deliberate re-backfill arrives under **new** keys (its side effects run again by
design), and a backfill interrupted by a crash resumes under a fresh run token, so rows it already
delivered can re-arrive with keys you have not seen. Gate side effects on document **state**, not on the
key alone, when duplicate effects are costly.

The response status classifies the outcome:

| Response | Outcome |
| --- | --- |
| 2xx | Delivered; the batch is acked. |
| 408, 429, 5xx, network errors, timeout | **Retryable** - the dispatcher retries with backoff. |
| Any other status | **Permanent** - the pipeline halts (the receiver rejected the payload). |

Batches larger than `MaxRecordsPerRequest` are split into sequential requests in commit order; a failing
chunk stops the delivery and the whole batch is redelivered after backoff.

## Compression

The JSON envelope compresses well (typically 80–90% smaller), which matters most during backfill bursts.
Opt in with:

```csharp
o.Compression = HttpSinkCompression.Gzip; // or Brotli
```

Requests then carry `Content-Encoding: gzip` (or `br`), so the receiver must decompress - in ASP.NET Core,
enable the [request decompression middleware](https://learn.microsoft.com/aspnet/core/fundamentals/middleware/request-decompression):

```csharp
builder.Services.AddRequestDecompression();
// ...
app.UseRequestDecompression();
```

## Verifying signatures

As an additional security measure, you can set `SigningSecret` and sign every request with a HMAC-SHA256 of the exact body:

```
X-Wallaby-Signature: sha256=<lowercase hex>
```

Verify it before trusting the payload:

```csharp
app.MapPost("/wallaby", async (HttpRequest request) =>
{
    using var buffer = new MemoryStream();
    await request.Body.CopyToAsync(buffer);
    var body = buffer.ToArray();

    var expected = $"sha256={Convert.ToHexStringLower(HMACSHA256.HashData(secretBytes, body))}";
    if (request.Headers["X-Wallaby-Signature"] != expected)
    {
        return Results.Unauthorized();
    }

    var envelope = JsonDocument.Parse(body);
    // apply envelope.RootElement.GetProperty("records") idempotently...
    return Results.Ok();
});
```

The HMAC signature is computed over the **uncompressed** payload, so signature verification works
unchanged against the body your endpoint reads after the middleware has decompressed it.

## NativeAOT

The envelope structure and common scalar document values (strings, numbers, booleans, `Guid`, date/time
types, byte arrays, nested dictionaries, and sequences of these) are written without reflection. Any other
value type is serialized through `SerializerOptions`; on trimmed/NativeAOT hosts, point it at a
source-generated context covering the types your transforms emit:

```csharp
o.SerializerOptions = new JsonSerializerOptions { TypeInfoResolver = MyJsonContext.Default };
```

Without it, non-scalar values fall back to reflection-based serialization (fine on JIT hosts) and fail
delivery permanently on AOT with an error naming the offending field.
