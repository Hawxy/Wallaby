---
description: "POSTing change batches to any HTTP endpoint as a JSON envelope, with named-client auth, Standard Webhooks signing, and idempotency keys."
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
           o.SigningSecret = secret; // optional Standard Webhooks signing ("whsec_...")
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
| `SigningSecret` | `null` | Enables [Standard Webhooks request signing](#verifying-signatures) (`whsec_...`). |
| `PreviousSigningSecret` | `null` | Second signature during [key rotation](#verifying-signatures). |
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

## Redirects

The sink never follows redirects as following one rewrites the POST to a GET and drops the body, while a
2xx from the target acknowledges a batch that was never delivered. Redirect following is disabled on the
sink's default named client, and a 3xx response fails **permanently** naming
the `Location`. Fix this by pointing `Endpoint` at the final URL instead.

A bring-your-own `HttpClientName` client is never reconfigured, so it must not follow redirects itself. 
As a defense, a success whose final request URI differs from the
URI the request was dispatched with is treated as a followed redirect and also fails permanently.

## The envelope

Each request is a JSON envelope; `records` preserves commit order:

```json
{
  "type": "wallaby.changes",
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

Top-level fields:

| Field | Meaning |
| --- | --- |
| `type` | Always `wallaby.changes`. |
| `sink` | The sink's registered name. |
| `sentAt` | When this request was sent - **per attempt**: a retried delivery re-sends the same records with a fresh `sentAt` (and, [when signed](#verifying-signatures), the same `webhook-id`). Treat requests with an equal `webhook-id` as the same delivery. |
| `annotations` | Present when `Annotations` is configured: those static key/values. |
| `records` | The change records, in commit order. |

Each record:

| Field | Meaning |
| --- | --- |
| `operation` | `upsert` (apply `document` under `id`) or `delete` (remove `id`). |
| `id` | The document id the operation targets. |
| `idempotencyKey` | An opaque string unique to each delivered change - store it to [reject redelivered duplicates](#delivery-semantics). A backfill row's key embeds a per-run token (echoed as `metadata.backfillRunId`): stable within one run, **new for every run**, so a re-backfill (e.g. a `WithBackfillVersion` bump) is never suppressed by stored keys. |
| `destination` | The mapping's `ToDestination(...)` value (or a [`ScopedDestination`](/providers/entity-framework-core/multi-tenancy) result); `null` when the mapping declares none. |
| `document` | The transform's document; upserts only, a delete carries none. |
| `metadata.schema`, `metadata.table` | The source table the change came from. |
| `metadata.action` | What the change meant in the source model: `insert`, `update`, `delete`, or `read` (a backfill row). Providers may substitute meaning - e.g. Marten surfaces a soft-delete `UPDATE` as `delete` - so it can differ from the raw WAL operation. |
| `metadata.commitLsn`, `metadata.commitIdx` | The change's commit position; `(commitLsn, commitIdx)` orders live changes. `commitLsn` is a string - the value can exceed the safe-integer range of JavaScript consumers. Backfill records have `commitLsn: "0"`. |
| `metadata.commitTimestamp` | The source transaction's commit time; omitted on backfill records. |
| `metadata.isBackfill` | `true` on rows delivered by a [backfill](/backfill) rather than live replication. |
| `metadata.backfillRunId` | The backfill run's per-run token; only on backfill rows. |

Two contract rules for receivers:

- **Ignore unknown fields.** New fields are added to the envelope additively (and some, like
  `metadata.backfillRunId`, appear only when relevant). A receiver that rejects or fails on
  unrecognized properties will break on upgrades that are compatible by contract.
- Receivers that want spec-shaped, one-event-per-request traffic can set `MaxRecordsPerRequest = 1`. The
  envelope stays the same but each request carries a single record.

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

Set `SigningSecret` and every request is signed per the
[Standard Webhooks](https://www.standardwebhooks.com/) specification, so any spec-conformant
verification library can check it:

```
webhook-id: msg_<hex>
webhook-timestamp: <unix seconds>
webhook-signature: v1,<base64> [v1,<base64>]
```

The signature is the HMAC-SHA256 of `{id}.{timestamp}.{body}`. The secret must be in the standard
format (base64, optionally prefixed `whsec_`) and decode to at least 16 key bytes, or the sink fails at
startup - an empty or too-short secret (an unset environment variable binds to `""`) would otherwise sign
every request with a key an attacker can guess. Optionally generate one with:

```csharp
var secret = "whsec_" + Convert.ToBase64String(RandomNumberGenerator.GetBytes(24));
```

Verify with a Standard Webhooks library - for .NET, the
[`StandardWebhooks`](https://www.nuget.org/packages/StandardWebhooks) package:

```csharp
app.MapPost("/wallaby", async (HttpRequest request) =>
{
    using var reader = new StreamReader(request.Body);
    var body = await reader.ReadToEndAsync();

    var webhook = new StandardWebhook(secret); // e.g. from configuration
    try
    {
        webhook.Verify(body, request.Headers); // checks signature + timestamp tolerance
    }
    catch (WebhookVerificationException)
    {
        return Results.Unauthorized();
    }

    var envelope = JsonDocument.Parse(body);
    // apply envelope.RootElement.GetProperty("records") idempotently...
    return Results.Ok();
});
```

**Key rotation:** set the new secret in `SigningSecret` and move the old one to
`PreviousSigningSecret`. Every request then carries a signature for each, so receivers can switch
whenever within the rotation window. Clear `PreviousSigningSecret` once all receivers are moved.

The signature is computed over the **uncompressed** payload, so verification works unchanged
against the body your endpoint reads after the middleware has decompressed it.

## NativeAOT

The envelope structure and common scalar document values (strings, numbers, booleans, `Guid`, date/time
types, byte arrays, nested dictionaries, `ReadOnlyMemory<float>` vectors, and sequences of these) are written without reflection. Any other
value type is serialized through `SerializerOptions`; on trimmed/NativeAOT hosts, point it at a
source-generated context covering the types your transforms emit:

```csharp
o.SerializerOptions = new JsonSerializerOptions { TypeInfoResolver = MyJsonContext.Default };
```

Without it, non-scalar values fall back to reflection-based serialization (fine on JIT hosts) and fail
delivery permanently on AOT with an error naming the offending field.
