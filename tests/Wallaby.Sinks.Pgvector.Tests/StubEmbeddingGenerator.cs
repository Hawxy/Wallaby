using Microsoft.Extensions.AI;

namespace Wallaby.Sinks.Pgvector.Tests;

/// <summary>
/// Deterministic <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/>: records every batch, embeds a
/// text as <c>[length, 1]</c> (override via <see cref="VectorFor"/>), and throws queued
/// <see cref="Failures"/> first, one per call. Thread-safe, so it works with concurrent sub-batches.
/// </summary>
internal sealed class StubEmbeddingGenerator : IEmbeddingGenerator<string, Embedding<float>>
{
    private readonly Lock _lock = new();

    public int Calls { get; private set; }
    public List<string[]> Batches { get; } = [];
    public Queue<Exception> Failures { get; } = new();
    public Func<string, float[]> VectorFor { get; set; } = text => [text.Length, 1f];

    public Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEnumerable<string> values, EmbeddingGenerationOptions? options = null, CancellationToken cancellationToken = default)
    {
        var texts = values.ToArray();
        lock (_lock)
        {
            Calls++;
            if (Failures.TryDequeue(out var failure))
            {
                throw failure;
            }
            Batches.Add(texts);
        }
        return Task.FromResult(new GeneratedEmbeddings<Embedding<float>>(
            texts.Select(t => new Embedding<float>(VectorFor(t)))));
    }

    public object? GetService(Type serviceType, object? serviceKey = null) => null;

    public void Dispose() { }
}
