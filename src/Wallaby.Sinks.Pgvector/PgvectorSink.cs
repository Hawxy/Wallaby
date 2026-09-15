using Npgsql;
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
    private readonly PgvectorTables _tables;
    private readonly PgvectorRowBuilder _rows;

    /// <summary>Creates a sink that delivers to the database described by <paramref name="options"/>.</summary>
    /// <param name="name">The sink's registration name (used for routing, telemetry, and test replacement).</param>
    /// <param name="options">Connection, table, and embedding settings.</param>
    public PgvectorSink(string name, PgvectorSinkOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);
        PgvectorBuilderExtensions.Validate(options);
        Name = name;
        _options = options;
        var builder = new NpgsqlDataSourceBuilder(options.ConnectionString);
        builder.UseVector();
        options.ConfigureDataSource?.Invoke(builder);
        _dataSource = builder.Build();
        _tables = new PgvectorTables(name, _dataSource, options);
        _rows = new PgvectorRowBuilder(options, _tables);
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken ct)
    {
        if (_options.CreateExtension)
        {
            await _tables.EnsureExtensionAsync(ct);
        }
        if (_options.CreateTable && _options.DefaultTable is { } table)
        {
            await _tables.EnsureTableAsync(table, ct);
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
        _tables.RequireValidTable(table);
        await _tables.PurgeAsync(table, ct);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _dataSource.DisposeAsync();

    private async Task DeliverTableAsync(string table, Dictionary<string, SinkRecord> records, CancellationToken ct)
    {
        _tables.RequireValidTable(table);
        if (_options.CreateTable)
        {
            await _tables.EnsureTableAsync(table, ct);
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

        var rows = await _rows.BuildAsync(table, upserts, ct);
        await _tables.WriteAsync(table, rows, deletes, ct);
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

    private DeliveryResult Classify(Exception ex) => ex switch
    {
        PostgresException pg when !pg.IsTransient =>
            DeliveryResult.Permanent($"Postgres rejected the delivery for sink '{Name}': {pg.MessageText}", pg),
        NpgsqlException or TimeoutException or System.IO.IOException or System.Net.Sockets.SocketException =>
            DeliveryResult.Retry($"Transient database failure for sink '{Name}': {ex.Message}", ex),
        _ => DeliveryResult.Permanent($"Delivery failed for sink '{Name}': {ex.Message}", ex),
    };
}
