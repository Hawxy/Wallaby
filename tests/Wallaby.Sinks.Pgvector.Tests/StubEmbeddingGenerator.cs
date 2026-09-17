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

    /// <summary>Time each call waits (honouring cancellation) before answering; simulates a slow provider.</summary>
    public TimeSpan Delay { get; set; }

    public async Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEnumerable<string> values, EmbeddingGenerationOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (Delay > TimeSpan.Zero)
        {
            await Task.Delay(Delay, cancellationToken);
        }
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
        return new GeneratedEmbeddings<Embedding<float>>(texts.Select(t => new Embedding<float>(VectorFor(t))));
    }

    public object? GetService(Type serviceType, object? serviceKey = null) => null;

    public void Dispose() { }
}
