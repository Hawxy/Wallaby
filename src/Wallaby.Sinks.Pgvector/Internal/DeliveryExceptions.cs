namespace Wallaby.Sinks.Pgvector.Internal;

/// <summary>Delivery failure that must halt the pipeline rather than retry.</summary>
internal sealed class PermanentDeliveryException(string message, Exception? inner = null)
    : Exception(message, inner);

/// <summary>Embedding-provider failure, carrying its retryability classification.</summary>
internal sealed class EmbeddingException(bool transient, Exception inner) : Exception(inner.Message, inner)
{
    public bool Transient { get; } = transient;
}
