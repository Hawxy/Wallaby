using System.Text;
using System.Text.Json;
using Microsoft.Extensions.AI;
using Npgsql;
using NpgsqlTypes;
using Pgvector;
using Pgvector.Npgsql;
using Wallaby.Abstractions;
using Wallaby.Sinks.Pgvector.Internal;

namespace Wallaby.Sinks.Pgvector;

/// <summary>
/// Delivers documents into per-destination pgvector tables shaped <c>(id text primary key, text_hash
/// text, embedding vector(N), document jsonb, updated_at timestamptz)</c>. Upserts are idempotent by
/// id and deletes remove by id, so redelivery converges. With an embedding generator configured the
/// sink embeds at delivery time and re-embeds only rows whose text hash changed - the destination
/// table doubles as the durable embedding cache, so restarts, failovers, and re-backfills never
/// re-embed unchanged text. Embedding and transient database failures surface as retryable delivery
/// results, riding the dispatcher's backoff.
/// </summary>
public sealed class PgvectorSink : ISink, ISinkInitializer, ISinkPurger, IAsyncDisposable
{
    private readonly PgvectorSinkOptions _options;
    private readonly NpgsqlDataSource _dataSource;
    private readonly HashSet<string> _ensuredTables = [];

    /// <summary>Creates a sink that delivers to the database described by <paramref name="options"/>.</summary>
    /// <param name="name">The sink's registration name (used for routing, telemetry, and test replacement).</param>
    /// <param name="options">Connection, table, and embedding settings.</param>
    public PgvectorSink(string name, PgvectorSinkOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);
        Name = name;
        _options = options;
        var builder = new NpgsqlDataSourceBuilder(options.ConnectionString);
        builder.UseVector();
        options.ConfigureDataSource?.Invoke(builder);
        _dataSource = builder.Build();
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken ct)
    {
        if (_options.CreateExtension)
        {
            try
            {
                await using var cmd = _dataSource.CreateCommand("CREATE EXTENSION IF NOT EXISTS vector");
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.InsufficientPrivilege)
            {
                throw new WallabyConfigurationException(
                    $"Pgvector sink '{Name}' cannot create the 'vector' extension (insufficient privilege). " +
                    "Have a superuser run CREATE EXTENSION vector; on the destination database, or set " +
                    "CreateExtension = false once it exists.", ex);
            }
            catch (PostgresException ex) when (
                ex.SqlState is PostgresErrorCodes.UniqueViolation or PostgresErrorCodes.DuplicateObject)
            {
                // IF NOT EXISTS is not concurrency-safe; a concurrent creator winning means it exists.
            }
            // The data source may have loaded its type catalog before the extension existed (its first
            // connection is this CREATE EXTENSION); reload so the 'vector' type resolves for parameters.
            await _dataSource.ReloadTypesAsync(ct);
        }
        if (_options.CreateTable && _options.DefaultTable is { } table)
        {
            await EnsureTableAsync(table, ct);
        }
    }

    /// <inheritdoc />
    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
    {
        try
        {
            foreach (var (table, records) in GroupByTable(batch.Records))
            {
                await DeliverTableAsync(table, records, ct);
            }
            return DeliveryResult.Success;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (PermanentDeliveryException ex)
        {
            return DeliveryResult.Permanent(ex.Message, ex.InnerException);
        }
        catch (EmbeddingException ex)
        {
            var reason = $"Embedding failed for sink '{Name}': {ex.InnerException!.Message}";
            return ex.Transient
                ? DeliveryResult.Retry(reason, ex.InnerException)
                : DeliveryResult.Permanent(reason, ex.InnerException);
        }
        catch (Exception ex)
        {
            return Classify(ex);
        }
    }

    /// <inheritdoc />
    public async Task PurgeAsync(SinkPurgeRequest request, CancellationToken ct)
    {
        var table = request.Destination ?? _options.DefaultTable
            ?? throw new WallabyConfigurationException(
                $"Pgvector sink '{Name}' cannot purge for '{request.QualifiedTableName}': the mapping has no " +
                "destination and the sink has no DefaultTable.");
        RequireValidTable(table);
        try
        {
            await using var cmd = _dataSource.CreateCommand($"DELETE FROM {Qualified(table)}");
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            // Nothing to purge; the table is created on initialization or first delivery.
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _dataSource.DisposeAsync();

    internal static bool IsValidIdentifier(string? value)
        => value is { Length: > 0 and <= 63 } && value.All(c => char.IsAsciiLetterOrDigit(c) || c == '_');

    private sealed record Row(string Id, string? Hash, Vector? Vector, bool KeepStoredVector, string DocumentJson);

    private async Task DeliverTableAsync(string table, Dictionary<string, SinkRecord> records, CancellationToken ct)
    {
        RequireValidTable(table);
        if (_options.CreateTable)
        {
            await EnsureTableAsync(table, ct);
        }

        var deletes = new List<string>();
        var upserts = new List<SinkRecord>();
        foreach (var record in records.Values)
        {
            if (record.IsDeletion)
            {
                deletes.Add(record.DocumentId);
            }
            else
            {
                upserts.Add(record);
            }
        }

        var rows = _options.EmbeddingGenerator is null
            ? BuildPassThroughRows(upserts)
            : await BuildEmbeddedRowsAsync(table, upserts, ct);

        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        foreach (var chunk in rows.Chunk(_options.MaxRowsPerBatch))
        {
            await using var writeBatch = new NpgsqlBatch(connection, transaction);
            foreach (var row in chunk)
            {
                // A KeepStoredVector row exists by definition (its stored hash matched), so the update
                // arm leaving embedding and text_hash untouched is the one that runs. Both arms guard
                // with IS DISTINCT FROM so an identical redelivery does not rewrite the tuple (no WAL
                // or dead-tuple churn on re-backfills of unchanged rows).
                var update = row.KeepStoredVector
                    ? "document = EXCLUDED.document, updated_at = now() " +
                      "WHERE t.document IS DISTINCT FROM EXCLUDED.document"
                    : "text_hash = EXCLUDED.text_hash, embedding = EXCLUDED.embedding, " +
                      "document = EXCLUDED.document, updated_at = now() " +
                      "WHERE (t.text_hash, t.embedding, t.document) IS DISTINCT FROM " +
                      "(EXCLUDED.text_hash, EXCLUDED.embedding, EXCLUDED.document)";
                var cmd = new NpgsqlBatchCommand(
                    $"INSERT INTO {Qualified(table)} AS t (id, text_hash, embedding, document) " +
                    $"VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET {update}");
                cmd.Parameters.Add(new NpgsqlParameter { Value = row.Id });
                cmd.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Hash ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                // A Vector value infers the vector type via the plugin; a bare null is sent untyped and
                // the server infers it from the target column.
                cmd.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Vector ?? DBNull.Value });
                cmd.Parameters.Add(new NpgsqlParameter { Value = row.DocumentJson, NpgsqlDbType = NpgsqlDbType.Jsonb });
                writeBatch.BatchCommands.Add(cmd);
            }
            await writeBatch.ExecuteNonQueryAsync(ct);
        }
        if (deletes.Count > 0)
        {
            await using var delete = new NpgsqlCommand(
                $"DELETE FROM {Qualified(table)} WHERE id = ANY($1)", connection, transaction);
            delete.Parameters.Add(new NpgsqlParameter { Value = deletes.ToArray() });
            await delete.ExecuteNonQueryAsync(ct);
        }
        await transaction.CommitAsync(ct);
    }

    private List<Row> BuildPassThroughRows(List<SinkRecord> upserts)
    {
        var rows = new List<Row>(upserts.Count);
        using var buffer = new MemoryStream();
        using var writer = new Utf8JsonWriter(buffer);
        foreach (var record in upserts)
        {
            Vector? stored = null;
            if (record.Document!.TryGetValue(_options.VectorField, out var value) && value is not null)
            {
                if (!PgvectorFormat.TryGetVector(value, out var vector))
                {
                    throw new PermanentDeliveryException(
                        $"Document '{record.DocumentId}' field '{_options.VectorField}' has type " +
                        $"'{value.GetType()}'; expected ReadOnlyMemory<float> or float[].");
                }
                stored = RequireDimensions(vector, record.DocumentId);
            }
            rows.Add(new Row(record.DocumentId, Hash: null, stored, KeepStoredVector: false,
                BuildDocumentJson(record, _options.VectorField, buffer, writer)));
        }
        return rows;
    }

    private async Task<List<Row>> BuildEmbeddedRowsAsync(string table, List<SinkRecord> upserts, CancellationToken ct)
    {
        var rows = new List<Row>(upserts.Count);
        if (upserts.Count == 0)
        {
            return rows;
        }

        // The destination is the cache: compare stored hashes and embed only what changed.
        var storedHashes = await LoadStoredHashesAsync(table, upserts, ct);
        var pendingTexts = new List<string>();
        var pendingRows = new List<int>();
        using (var buffer = new MemoryStream())
        using (var writer = new Utf8JsonWriter(buffer))
        {
            foreach (var record in upserts)
            {
                var text = _options.EmbedText!(record.Document!);
                var json = BuildDocumentJson(record, excludeField: null, buffer, writer);
                if (string.IsNullOrEmpty(text))
                {
                    rows.Add(new Row(record.DocumentId, Hash: null, Vector: null, KeepStoredVector: false, json));
                    continue;
                }

                var hash = PgvectorFormat.TextHash(_options.EmbeddingVersion!, text);
                if (storedHashes.TryGetValue(record.DocumentId, out var stored) && stored == hash)
                {
                    rows.Add(new Row(record.DocumentId, hash, Vector: null, KeepStoredVector: true, json));
                    continue;
                }

                pendingRows.Add(rows.Count);
                pendingTexts.Add(text);
                rows.Add(new Row(record.DocumentId, hash, Vector: null, KeepStoredVector: false, json));
            }
        }

        // Sub-batches fill disjoint slices of the vectors array, so they can run concurrently up to
        // MaxEmbeddingConcurrency (default 1: sequential).
        var vectors = new Vector[pendingTexts.Count];
        var subBatches = new List<(int Offset, int Count)>();
        for (var offset = 0; offset < pendingTexts.Count; offset += _options.MaxEmbeddingBatchSize)
        {
            subBatches.Add((offset, Math.Min(_options.MaxEmbeddingBatchSize, pendingTexts.Count - offset)));
        }
        await Parallel.ForEachAsync(subBatches,
            new ParallelOptions { MaxDegreeOfParallelism = _options.MaxEmbeddingConcurrency, CancellationToken = ct },
            async (subBatch, token) =>
            {
                var texts = pendingTexts.GetRange(subBatch.Offset, subBatch.Count);
                GeneratedEmbeddings<Embedding<float>> embeddings;
                try
                {
                    embeddings = await _options.EmbeddingGenerator!.GenerateAsync(texts, options: null, token);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new EmbeddingException(IsTransientEmbedding(ex), ex);
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
                    vectors[index] = RequireDimensions(embeddings[i].Vector, rows[pendingRows[index]].Id);
                }
            });
        for (var i = 0; i < pendingRows.Count; i++)
        {
            rows[pendingRows[i]] = rows[pendingRows[i]] with { Vector = vectors[i] };
        }
        return rows;
    }

    private async Task<Dictionary<string, string>> LoadStoredHashesAsync(
        string table, List<SinkRecord> upserts, CancellationToken ct)
    {
        var hashes = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var cmd = _dataSource.CreateCommand(
            $"SELECT id, text_hash FROM {Qualified(table)} WHERE id = ANY($1) AND text_hash IS NOT NULL");
        cmd.Parameters.Add(new NpgsqlParameter { Value = upserts.Select(u => u.DocumentId).ToArray() });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            hashes[reader.GetString(0)] = reader.GetString(1);
        }
        return hashes;
    }

    private Vector RequireDimensions(ReadOnlyMemory<float> vector, string documentId)
        => vector.Length == _options.Dimensions
            ? new Vector(vector)
            : throw new PermanentDeliveryException(
                $"Document '{documentId}' has a {vector.Length}-dimensional vector; the sink is configured " +
                $"for vector({_options.Dimensions}).");

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
        SinkEnvelopeJson.WriteDocument(writer, document, record.DocumentId, _options.SerializerOptions);
        writer.Flush();
        return Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
    }

    // Last write per id wins within the batch (same as replaying it row-by-row), grouped per table in
    // first-seen order.
    private Dictionary<string, Dictionary<string, SinkRecord>> GroupByTable(IReadOnlyList<SinkRecord> records)
    {
        var byTable = new Dictionary<string, Dictionary<string, SinkRecord>>(StringComparer.Ordinal);
        foreach (var record in records)
        {
            var table = record.Destination ?? _options.DefaultTable
                ?? throw new PermanentDeliveryException(
                    $"A record for sink '{Name}' has no destination and the sink declares no DefaultTable. " +
                    "Set ToDestination(...) on the mapping or DefaultTable on the sink.");
            if (!byTable.TryGetValue(table, out var byId))
            {
                byTable[table] = byId = new Dictionary<string, SinkRecord>(StringComparer.Ordinal);
            }
            byId[record.DocumentId] = record;
        }
        return byTable;
    }

    private async Task EnsureTableAsync(string table, CancellationToken ct)
    {
        lock (_ensuredTables)
        {
            if (_ensuredTables.Contains(table))
            {
                return;
            }
        }
        try
        {
            await using var cmd = _dataSource.CreateCommand(
                $"""
                 CREATE TABLE IF NOT EXISTS {Qualified(table)} (
                     id         text PRIMARY KEY,
                     text_hash  text,
                     embedding  vector({_options.Dimensions}),
                     document   jsonb NOT NULL,
                     updated_at timestamptz NOT NULL DEFAULT now()
                 )
                 """);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException ex) when (
            ex.SqlState is PostgresErrorCodes.UniqueViolation or PostgresErrorCodes.DuplicateTable)
        {
            // IF NOT EXISTS is not concurrency-safe; a concurrent creator winning means it exists.
        }
        lock (_ensuredTables)
        {
            _ensuredTables.Add(table);
        }
    }

    private void RequireValidTable(string table)
    {
        if (!IsValidIdentifier(table))
        {
            throw new PermanentDeliveryException(
                $"Destination '{table}' is not a valid pgvector table name for sink '{Name}': use 1-63 " +
                "characters of [a-zA-Z0-9_].");
        }
    }

    private string Qualified(string table) => $"\"{_options.Schema}\".\"{table}\"";

    private bool IsTransientEmbedding(Exception ex)
        => _options.IsTransientEmbeddingError?.Invoke(ex)
           ?? ex is not (ArgumentException or NotSupportedException);

    private DeliveryResult Classify(Exception ex) => ex switch
    {
        PostgresException pg when !pg.IsTransient =>
            DeliveryResult.Permanent($"Postgres rejected the delivery for sink '{Name}': {pg.MessageText}", pg),
        NpgsqlException or TimeoutException or System.IO.IOException or System.Net.Sockets.SocketException =>
            DeliveryResult.Retry($"Transient database failure for sink '{Name}': {ex.Message}", ex),
        _ => DeliveryResult.Permanent($"Delivery failed for sink '{Name}': {ex.Message}", ex),
    };

    private sealed class PermanentDeliveryException(string message, Exception? inner = null)
        : Exception(message, inner);

    private sealed class EmbeddingException(bool transient, Exception inner) : Exception(inner.Message, inner)
    {
        public bool Transient { get; } = transient;
    }
}
