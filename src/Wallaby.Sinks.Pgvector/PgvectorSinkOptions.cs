using System.Text.Json;
using Microsoft.Extensions.AI;
using Npgsql;

namespace Wallaby.Sinks.Pgvector;

/// <summary>
/// Settings for a pgvector sink. The sink writes one row per document into a per-destination table
/// shaped <c>(id text primary key, text_hash text, embedding vector(N), document jsonb, updated_at
/// timestamptz)</c>. With an <see cref="EmbeddingGenerator"/> configured the sink embeds at delivery
/// time, re-embedding only rows whose <see cref="EmbedText"/> output changed (hash-gated against the
/// destination table itself); without one, the transform supplies the vector via
/// <see cref="VectorField"/>.
/// </summary>
public sealed class PgvectorSinkOptions
{
    /// <summary>Connection string of the destination database (often not the CDC source).</summary>
    public required string ConnectionString { get; set; }

    /// <summary>Extra data-source configuration (TLS callbacks, loggers, ...).</summary>
    public Action<NpgsqlDataSourceBuilder>? ConfigureDataSource { get; set; }

    /// <summary>Schema holding the destination tables.</summary>
    public string Schema { get; set; } = "public";

    /// <summary>Table used when a routed record has no destination.</summary>
    public string? DefaultTable { get; set; }

    /// <summary>
    /// Dimension count of the <c>embedding vector(N)</c> column; every stored vector must match.
    /// </summary>
    public required int Dimensions { get; set; }

    /// <summary>
    /// Create missing destination tables (and, with <see cref="CreateExtension"/>, the extension) on
    /// initialization and on first delivery to a runtime destination.
    /// </summary>
    public bool CreateTable { get; set; } = true;

    /// <summary>Run <c>CREATE EXTENSION IF NOT EXISTS vector</c> during initialization.</summary>
    public bool CreateExtension { get; set; } = true;

    /// <summary>
    /// Embeds at delivery time. Configure together with <see cref="EmbedText"/> and
    /// <see cref="EmbeddingVersion"/>; leave null to have transforms supply vectors via
    /// <see cref="VectorField"/> instead.
    /// </summary>
    public IEmbeddingGenerator<string, Embedding<float>>? EmbeddingGenerator { get; set; }

    /// <summary>
    /// Selects the text to embed from a document's field bag. Null/empty output stores the row with a
    /// null vector. Required when <see cref="EmbeddingGenerator"/> is set.
    /// </summary>
    public Func<IReadOnlyDictionary<string, object?>, string?>? EmbedText { get; set; }

    /// <summary>
    /// Identifies the embedding model and prompt shape, e.g. <c>"text-embedding-3-small/1"</c>. Folded
    /// into the stored text hash, so changing it re-embeds rows as they re-deliver; pair it with the
    /// mapping's <c>WithBackfillVersion(..., purgeOnChange: true)</c> to re-embed the whole corpus at
    /// once. Required when <see cref="EmbeddingGenerator"/> is set.
    /// </summary>
    public string? EmbeddingVersion { get; set; }

    /// <summary>Max texts per embedding call; larger sets split into multiple calls.</summary>
    public int MaxEmbeddingBatchSize { get; set; } = 96;

    /// <summary>
    /// Max embedding calls in flight at once. The default (1) sends calls sequentially; raise it to
    /// overlap calls on large backfills when the provider's rate limits allow.
    /// </summary>
    public int MaxEmbeddingConcurrency { get; set; } = 1;

    /// <summary>
    /// Classifies an embedding-provider exception as retryable (delivery backs off and retries) versus
    /// permanent (the pipeline halts). Null uses the default: everything is retryable except
    /// <see cref="ArgumentException"/> and <see cref="NotSupportedException"/>. Narrow it when your
    /// provider surfaces typed auth/quota errors that retrying cannot fix.
    /// </summary>
    public Func<Exception, bool>? IsTransientEmbeddingError { get; set; }

    /// <summary>
    /// Without an <see cref="EmbeddingGenerator"/>: the document field carrying the vector
    /// (<c>ReadOnlyMemory&lt;float&gt;</c> or <c>float[]</c>). The field is stored in the vector
    /// column and dropped from the jsonb payload; a document without it stores a null vector.
    /// </summary>
    public string VectorField { get; set; } = "embedding";

    /// <summary>Rows per database round-trip; larger batches split into sequential command batches.</summary>
    public int MaxRowsPerBatch { get; set; } = 500;

    /// <summary>Serializer for document values beyond the natively written scalar types (required for such values on NativeAOT hosts).</summary>
    public JsonSerializerOptions? SerializerOptions { get; set; }
}
