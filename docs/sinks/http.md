---
title: "Send Postgres changes to a webhook from .NET"
description: "POST Postgres inserts, updates and deletes to any HTTP endpoint from C#: JSON envelope, Standard Webhooks signing, idempotency keys, at-least-once delivery."
---

# HTTP (Webhook) Sink

Send Postgres changes to any webhook or HTTP endpoint from your .NET application. The
`Wallaby.Sinks.Http` package POSTs batches of changes as a JSON envelope of upsert/delete records.
Retries, ordering, and at-least-once delivery are handled by the pipeline; your receiver just
applies records idempotently.

## Quickstart

```bash
dotnet add package Wallaby.Sinks.Http
```

Register Wallaby, point it at a storage provider, add the sink, and map an entity. The mapping's
destination is echoed on every record in [the envelope](#the-envelope).

::: code-group

```csharp [EF Core]
builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>()
       .UseConnectionString(conn)
       .AddHttpSink("webhook", o =>
       {
           o.Endpoint = "https://api.example.com/wallaby";
           o.SigningSecret = secret;
       })
       .WithMappings(sink => sink
           .Map<Product>()
           .ToDestination("products")
           .UsingTransform(/* ... */));
});
```

```csharp [Marten]
builder.Services.AddWallaby(cdc =>
{
    cdc.UseMarten()
       .UseConnectionString(conn)
       .AddHttpSink("webhook", o =>
       {
           o.Endpoint = "https://api.example.com/wallaby";
           o.SigningSecret = secret;
       })
       .WithMappings(sink => sink
           .Map<Product>()
           .ToDestination("products")
           .UsingTransform(/* ... */));
});
```

```csharp [Plain tables]
builder.Services.AddWallaby(cdc =>
{
    cdc.UseTables(tables => tables.Add<Product>())
       .UseConnectionString(conn)
       .AddHttpSink("webhook", o =>
       {
           o.Endpoint = "https://api.example.com/wallaby";
           o.SigningSecret = secret;
       })
       .WithMappings(sink => sink
           .Map<Product>()
           .ToDestination("products")
           .UsingTransform(/* ... */));
});
```

:::

The transform shapes each change into the record body; see [mappings](/mappings#transforms). For the
Postgres server settings Wallaby needs, see
[getting started](/getting-started#server-prerequisites).

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
| `Timeout` | `30s` | Per-request timeout; composes with any timeout on the named client. |
| `SerializerOptions` | `null` | Serializer for non-scalar document values (see [NativeAOT](#nativeaot)). |

## Authentication

Configure authentication on the sink's `IHttpClientFactory` **named client**. The sink itself only adds
the body and, when signing is enabled, its signature headers:

```csharp
builder.Services.AddHttpClient(HttpSink.ClientNameFor("webhook"))
    .ConfigureHttpClient(c => c.DefaultRequestHeaders.Add("X-Api-Key", apiKey))
    .AddHttpMessageHandler<OAuthTokenHandler>(); // your DelegatingHandler
```

## Redirects

The sink never follows redirects. Following one would turn the POST into a GET and drop the body, and
the 2xx from the redirect target would then acknowledge a batch that was never delivered. So redirect
following is disabled on the sink's default named client, and a 3xx response fails **permanently** with
an error naming the `Location`. To fix it, point `Endpoint` at the final URL.

If you supply your own client with `HttpClientName`, Wallaby doesn't reconfigure it, so make sure it
doesn't follow redirects either. As a safeguard, a success whose final request URI differs from the one
the request was sent to is treated as a followed redirect, and also fails permanently.

## The envelope

Each request body is a JSON envelope, with `records` in commit order:

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
| `sentAt` | When this request was sent, **per attempt**. A retried delivery re-sends the same records with a fresh `sentAt` (and, [when signed](#verifying-signatures), the same `webhook-id`), so treat requests with an equal `webhook-id` as the same delivery. |
| `annotations` | Present when `Annotations` is configured: those static key/values. |
| `records` | The change records, in commit order. |

Each record:

| Field | Meaning |
| --- | --- |
| `operation` | `upsert` (apply `document` under `id`) or `delete` (remove `id`). |
| `id` | The document id the operation targets. |
| `idempotencyKey` | An opaque string unique to each delivered change. Store it to [reject redelivered duplicates](#delivery-semantics). A backfill row's key embeds a per-run token (echoed as `metadata.backfillRunId`) that's stable within one run but **new for every run**, so a re-backfill (e.g. a `WithBackfillVersion` bump) is never suppressed by stored keys. |
| `destination` | The mapping's `ToDestination(...)` value (or a [`ScopedDestination`](/providers/entity-framework-core/multi-tenancy) result); `null` when the mapping declares none. |
| `document` | The transform's document. Upserts only; a delete carries none. |
| `metadata.schema`, `metadata.table` | The source table the change came from. |
| `metadata.action` | What the change meant in the source model: `insert`, `update`, `delete`, or `read` (a backfill row). Providers may reinterpret a change (e.g. Marten surfaces a soft-delete `UPDATE` as `delete`), so it can differ from the raw WAL operation. |
| `metadata.commitLsn`, `metadata.commitIdx` | The change's commit position; `(commitLsn, commitIdx)` orders live changes. `commitLsn` is a string, since the value can exceed JavaScript's safe-integer range. Backfill records have `commitLsn: "0"`. |
| `metadata.commitTimestamp` | The source transaction's commit time; omitted on backfill records. |
| `metadata.isBackfill` | `true` on rows delivered by a [backfill](/backfill) rather than live replication. |
| `metadata.backfillRunId` | The backfill run's per-run token; only on backfill rows. |

Receivers must **ignore unknown fields**. New fields are added to the envelope over time (and some,
like `metadata.backfillRunId`, only appear when relevant), so a receiver that rejects unrecognized
properties will break on upgrades that are compatible by contract.

If your receiver expects one event per request, set `MaxRecordsPerRequest = 1`. The envelope stays the
same, but each request carries a single record.

## Delivery semantics

Delivery is **at-least-once**: a crash can redeliver a batch your receiver already processed. Apply
records idempotently: upsert by `id`, delete by `id`, and treat a delete for an unknown id as success.

If your receiver has side effects beyond state (e.g. it sends an email per record), store each record's
`idempotencyKey` and skip keys you've already seen. `(commitLsn, commitIdx)` orders live changes.
Backfill rows come with two caveats:

- A deliberate re-backfill arrives under **new** keys, so its side effects run again (by design).
- A backfill interrupted by a crash resumes under a fresh run token, so rows it already delivered can
  re-arrive with keys you haven't seen.

When duplicate effects are costly, gate them on the document's **state**, not on the key alone.

The response status decides the outcome:

| Response | Outcome |
| --- | --- |
| 2xx | Delivered; the batch is acked. |
| 408, 429, 5xx, network errors, timeout | **Retryable**: the dispatcher retries with backoff. |
| Any other status | **Permanent**: the pipeline halts (the receiver rejected the payload). |

Batches larger than `MaxRecordsPerRequest` are split into sequential requests in commit order. If one
request fails, delivery stops and the whole batch is redelivered after backoff.

The sink can't purge, since a receiver has no "delete everything" contract. A
[purge-then-backfill](/backfill#purging-before-a-backfill) skips it with a warning, and documents whose
source rows disappeared without a delivered delete stay on the receiver.

## Compression

The JSON envelope compresses well (typically 80–90% smaller), which matters most during backfill bursts.
Opt in with:

```csharp
o.Compression = HttpSinkCompression.Gzip; // or Brotli
```

Requests then carry `Content-Encoding: gzip` (or `br`), so the receiver has to decompress them. In
ASP.NET Core, enable the
[request decompression middleware](https://learn.microsoft.com/aspnet/core/fundamentals/middleware/request-decompression):

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
startup. Without that check, an empty or too-short secret (an unset environment variable binds to `""`)
would sign every request with a key an attacker can guess. You can generate one with:

```csharp
var secret = "whsec_" + Convert.ToBase64String(RandomNumberGenerator.GetBytes(24));
```

Verify requests with any Standard Webhooks library, such as the
[`StandardWebhooks`](https://www.nuget.org/packages/StandardWebhooks) package for .NET:

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
`PreviousSigningSecret`. Every request then carries a signature for each, so receivers can switch at
any point within the rotation window. Clear `PreviousSigningSecret` once every receiver has switched.

The signature is computed over the **uncompressed** payload, so verification works unchanged against
the body your endpoint reads after the middleware has decompressed it.

## NativeAOT

The envelope structure and common document values are written without reflection: strings, numbers,
booleans, `Guid`, date/time types, byte arrays, nested dictionaries, `ReadOnlyMemory<float>` vectors,
and sequences of these. Any other value type is serialized through `SerializerOptions`. On
trimmed/NativeAOT hosts, point it at a source-generated context covering the types your transforms emit:

```csharp
o.SerializerOptions = new JsonSerializerOptions { TypeInfoResolver = MyJsonContext.Default };
```

Without it, non-scalar values fall back to reflection-based serialization (fine on JIT hosts) and fail
delivery permanently on AOT with an error naming the offending field.
