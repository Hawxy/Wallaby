using System.Text;
using System.Text.Json;
using Microsoft.Extensions.AI;
using Pgvector;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Pgvector.Internal;

/// <summary>One upsert for the write batch. <c>KeepStoredVector</c> marks a hash-gated row whose stored vector stays.</summary>
internal sealed record PgvectorRow(string Id, string? Hash, Vector? Vector, bool KeepStoredVector, string DocumentJson);

/// <summary>
/// Builds the rows a delivery writes: pass-through vector extraction, or hash-gated embedding where
/// only rows whose stored hash is missing or different reach the generator.
/// </summary>
internal sealed class PgvectorRowBuilder(PgvectorSinkOptions options, PgvectorTables tables)
{
    public Task<List<PgvectorRow>> BuildAsync(string table, List<SinkRecord> upserts, CancellationToken ct)
        => options.EmbeddingGenerator is null
            ? Task.FromResult(BuildPassThrough(upserts))
            : BuildEmbeddedAsync(table, upserts, ct);

    private List<PgvectorRow> BuildPassThrough(List<SinkRecord> upserts)
    {
        var rows = new List<PgvectorRow>(upserts.Count);
        using var buffer = new MemoryStream();
        using var writer = new Utf8JsonWriter(buffer);
        foreach (var record in upserts)
        {
            Vector? stored = null;
            if (record.Document!.TryGetValue(options.VectorField, out var value) && value is not null)
            {
                if (!PgvectorFormat.TryGetVector(value, out var vector))
                {
                    throw new PermanentDeliveryException(
                        $"Document '{record.DocumentId}' field '{options.VectorField}' has type " +
                        $"'{value.GetType()}'; expected ReadOnlyMemory<float> or float[].");
                }
                stored = RequireDimensions(vector, record.DocumentId);
            }
            rows.Add(new PgvectorRow(record.DocumentId, Hash: null, stored, KeepStoredVector: false,
                BuildDocumentJson(record, options.VectorField, buffer, writer)));
        }
        return rows;
    }

    private async Task<List<PgvectorRow>> BuildEmbeddedAsync(string table, List<SinkRecord> upserts, CancellationToken ct)
    {
        var rows = new List<PgvectorRow>(upserts.Count);
        if (upserts.Count == 0)
        {
            return rows;
        }

        // The destination is the cache: compare stored hashes and embed only what changed.
        var storedHashes = await tables.LoadStoredHashesAsync(
            table, upserts.Select(u => u.DocumentId).ToArray(), ct);
        var pendingTexts = new List<string>();
        var pendingRows = new List<int>();
        using (var buffer = new MemoryStream())
        using (var writer = new Utf8JsonWriter(buffer))
        {
            foreach (var record in upserts)
            {
                var text = options.EmbedText!(record.Document!);
                var json = BuildDocumentJson(record, excludeField: null, buffer, writer);
                if (string.IsNullOrEmpty(text))
                {
                    rows.Add(new PgvectorRow(record.DocumentId, Hash: null, Vector: null, KeepStoredVector: false, json));
                    continue;
                }

                var hash = PgvectorFormat.TextHash(options.EmbeddingVersion!, text);
                if (storedHashes.TryGetValue(record.DocumentId, out var stored) && stored == hash)
                {
                    rows.Add(new PgvectorRow(record.DocumentId, hash, Vector: null, KeepStoredVector: true, json));
                    continue;
                }

                pendingRows.Add(rows.Count);
                pendingTexts.Add(text);
                rows.Add(new PgvectorRow(record.DocumentId, hash, Vector: null, KeepStoredVector: false, json));
            }
        }

        var vectors = await EmbedAsync(pendingTexts, i => rows[pendingRows[i]].Id, ct);
        for (var i = 0; i < pendingRows.Count; i++)
        {
            rows[pendingRows[i]] = rows[pendingRows[i]] with { Vector = vectors[i] };
        }
        return rows;
    }

    // Sub-batches fill disjoint slices of the result array, so they can run concurrently up to
    // MaxEmbeddingConcurrency (default 1: sequential).
    private async Task<Vector[]> EmbedAsync(List<string> texts, Func<int, string> documentIdAt, CancellationToken ct)
    {
        var vectors = new Vector[texts.Count];
        var subBatches = new List<(int Offset, int Count)>();
        for (var offset = 0; offset < texts.Count; offset += options.MaxEmbeddingBatchSize)
        {
            subBatches.Add((offset, Math.Min(options.MaxEmbeddingBatchSize, texts.Count - offset)));
        }
        await Parallel.ForEachAsync(subBatches,
            new ParallelOptions { MaxDegreeOfParallelism = options.MaxEmbeddingConcurrency, CancellationToken = ct },
            async (subBatch, token) =>
            {
                GeneratedEmbeddings<Embedding<float>> embeddings;
                try
                {
                    embeddings = await options.EmbeddingGenerator!.GenerateAsync(
                        texts.GetRange(subBatch.Offset, subBatch.Count), options: null, token);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new EmbeddingException(IsTransient(ex), ex);
                }
                if (embeddings.Count != subBatch.Count)
                {
                    throw new PermanentDeliveryException(
                        $"The embedding generator returned {embeddings.Count} embeddings for {subBatch.Count} inputs; " +
                        "counts must match one-to-one.");
                }
                for (var i = 0; i < subBatch.Count; i++)
                {
                    var index = subBatch.Offset + i;
                    vectors[index] = RequireDimensions(embeddings[i].Vector, documentIdAt(index));
                }
            });
        return vectors;
    }

    private Vector RequireDimensions(ReadOnlyMemory<float> vector, string documentId)
        => vector.Length == options.Dimensions
            ? new Vector(vector)
            : throw new PermanentDeliveryException(
                $"Document '{documentId}' has a {vector.Length}-dimensional vector; the sink is configured " +
                $"for vector({options.Dimensions}).");

    // Callers own buffer and writer so one pair serves a whole batch; both are reset per document.
    private string BuildDocumentJson(SinkRecord record, string? excludeField, MemoryStream buffer, Utf8JsonWriter writer)
    {
        var document = record.Document!;
        if (excludeField is not null && document.ContainsKey(excludeField))
        {
            document = document.Where(f => f.Key != excludeField).ToDictionary(f => f.Key, f => f.Value);
        }
        buffer.SetLength(0);
        writer.Reset();
        SinkEnvelopeJson.WriteDocument(writer, document, record.DocumentId, options.SerializerOptions);
        writer.Flush();
        return Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
    }

    private bool IsTransient(Exception ex)
        => options.IsTransientEmbeddingError?.Invoke(ex)
           ?? ex is not (ArgumentException or NotSupportedException);
}
