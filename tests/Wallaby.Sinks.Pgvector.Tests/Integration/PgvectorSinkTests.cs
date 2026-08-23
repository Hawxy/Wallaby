using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Sinks.Pgvector.Tests.Integration.Infrastructure;

namespace Wallaby.Sinks.Pgvector.Tests.Integration;

[ClassDataSource<PgvectorFixture>(Shared = SharedType.PerTestSession)]
public class PgvectorSinkTests(PgvectorFixture pg)
{
    private static string UniqueTable() => $"t_{Guid.NewGuid():N}";

    private PgvectorSinkOptions Options(string table, Action<PgvectorSinkOptions>? mutate = null)
    {
        var options = new PgvectorSinkOptions
        {
            ConnectionString = pg.ConnectionString,
            Dimensions = 2,
            DefaultTable = table,
        };
        mutate?.Invoke(options);
        return options;
    }

    private PgvectorSinkOptions EmbedOptions(string table, StubEmbeddingGenerator generator, string version = "m/1")
        => Options(table, o =>
        {
            o.EmbeddingGenerator = generator;
            o.EmbedText = d => (string?)d.GetValueOrDefault("name");
            o.EmbeddingVersion = version;
        });

    private static ChangeMetadata Meta() =>
        new("public", "products", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, false);

    private static SinkRecord Upsert(string id, WallabyDocument document, string? destination = null)
        => new(destination, id, document, IsDeletion: false, Meta());

    private static SinkRecord Delete(string id, string? destination = null)
        => new(destination, id, Document: null, IsDeletion: true, Meta());

    private static SinkBatch Batch(params SinkRecord[] records) => new("pgv", records);

    private async Task<(string? Hash, string? Vector, string Json)?> RowAsync(string table, string id)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            $"SELECT text_hash, embedding::text, document::text FROM public.\"{table}\" WHERE id = $1");
        cmd.Parameters.Add(new NpgsqlParameter { Value = id });
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }
        return (reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.GetString(2));
    }

    [Test]
    public async Task Initialize_creates_the_extension_and_default_table()
    {
        var table = UniqueTable();
        await using var sink = new PgvectorSink("pgv", Options(table));

        await sink.InitializeAsync(CancellationToken.None);

        await using var cmd = pg.DataSource.CreateCommand($"SELECT to_regclass('public.{table}')::text");
        (await cmd.ExecuteScalarAsync()).ShouldNotBe(DBNull.Value);
    }

    [Test]
    public async Task Embed_mode_stores_vectors_and_hash_gates_redelivery()
    {
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        await using var sink = new PgvectorSink("pgv", EmbedOptions(table, generator));
        await sink.InitializeAsync(CancellationToken.None);

        var first = await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" }),
                  Upsert("2", new WallabyDocument { ["name"] = "cdef" })), CancellationToken.None);

        first.Status.ShouldBe(DeliveryStatus.Success);
        generator.Calls.ShouldBe(1);
        var row = await RowAsync(table, "1");
        row!.Value.Vector.ShouldBe("[2,1]");
        row.Value.Hash.ShouldNotBeNull();
        row.Value.Json.ShouldContain("\"ab\"");

        // Same text again: the stored hash matches, so no embedding call happens.
        var redelivery = await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "ab", ["extra"] = 7 })), CancellationToken.None);
        redelivery.Status.ShouldBe(DeliveryStatus.Success);
        generator.Calls.ShouldBe(1);
        (await RowAsync(table, "1"))!.Value.Json.ShouldContain("\"extra\"");

        // Changed text re-embeds.
        var changed = await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "abc" })), CancellationToken.None);
        changed.Status.ShouldBe(DeliveryStatus.Success);
        generator.Calls.ShouldBe(2);
        (await RowAsync(table, "1"))!.Value.Vector.ShouldBe("[3,1]");
    }

    private async Task<string> RowVersionAsync(string table, string id)
    {
        // xmin changes on any tuple rewrite, so an unchanged xmin proves the redelivery wrote nothing.
        await using var cmd = pg.DataSource.CreateCommand(
            $"SELECT xmin::text FROM public.\"{table}\" WHERE id = $1");
        cmd.Parameters.Add(new NpgsqlParameter { Value = id });
        return (string)(await cmd.ExecuteScalarAsync())!;
    }

    [Test]
    public async Task An_identical_redelivery_does_not_rewrite_the_row()
    {
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        await using var sink = new PgvectorSink("pgv", EmbedOptions(table, generator));
        await sink.InitializeAsync(CancellationToken.None);

        await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "ab", ["note"] = "x" })), CancellationToken.None);
        var version = await RowVersionAsync(table, "1");

        var redelivery = await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "ab", ["note"] = "x" })), CancellationToken.None);
        redelivery.Status.ShouldBe(DeliveryStatus.Success);
        (await RowVersionAsync(table, "1")).ShouldBe(version);

        // Same text but a changed document still updates the row (without re-embedding).
        await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "ab", ["note"] = "y" })), CancellationToken.None);
        (await RowVersionAsync(table, "1")).ShouldNotBe(version);
        (await RowAsync(table, "1"))!.Value.Json.ShouldContain("\"y\"");
        generator.Calls.ShouldBe(1);
    }

    [Test]
    public async Task An_identical_pass_through_redelivery_does_not_rewrite_the_row()
    {
        var table = UniqueTable();
        await using var sink = new PgvectorSink("pgv", Options(table));
        await sink.InitializeAsync(CancellationToken.None);
        var batch = Batch(Upsert("1", new WallabyDocument
        {
            ["name"] = "ab",
            ["embedding"] = new[] { 0.5f, -1f },
        }));

        await sink.DeliverAsync(batch, CancellationToken.None);
        var version = await RowVersionAsync(table, "1");

        (await sink.DeliverAsync(batch, CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);
        (await RowVersionAsync(table, "1")).ShouldBe(version);
    }

    [Test]
    public async Task Concurrent_embedding_sub_batches_store_every_vector()
    {
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        var options = EmbedOptions(table, generator);
        options.MaxEmbeddingBatchSize = 1;
        options.MaxEmbeddingConcurrency = 4;
        await using var sink = new PgvectorSink("pgv", options);
        await sink.InitializeAsync(CancellationToken.None);

        var result = await sink.DeliverAsync(Batch(
            Upsert("1", new WallabyDocument { ["name"] = "a" }),
            Upsert("2", new WallabyDocument { ["name"] = "bb" }),
            Upsert("3", new WallabyDocument { ["name"] = "ccc" }),
            Upsert("4", new WallabyDocument { ["name"] = "dddd" }),
            Upsert("5", new WallabyDocument { ["name"] = "eeeee" })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        generator.Calls.ShouldBe(5);
        (await RowAsync(table, "1"))!.Value.Vector.ShouldBe("[1,1]");
        (await RowAsync(table, "5"))!.Value.Vector.ShouldBe("[5,1]");
    }

    [Test]
    public async Task The_stored_hash_survives_a_new_sink_instance()
    {
        // A restarted host (or another node) skips re-embedding: the destination is the cache.
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        await using (var sink = new PgvectorSink("pgv", EmbedOptions(table, generator)))
        {
            await sink.InitializeAsync(CancellationToken.None);
            await sink.DeliverAsync(Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" })), CancellationToken.None);
        }
        generator.Calls.ShouldBe(1);

        await using var restarted = new PgvectorSink("pgv", EmbedOptions(table, generator));
        var result = await restarted.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        generator.Calls.ShouldBe(1);
    }

    [Test]
    public async Task An_embedding_version_change_re_embeds_the_same_text()
    {
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        await using (var sink = new PgvectorSink("pgv", EmbedOptions(table, generator)))
        {
            await sink.InitializeAsync(CancellationToken.None);
            await sink.DeliverAsync(Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" })), CancellationToken.None);
        }

        await using var bumped = new PgvectorSink("pgv", EmbedOptions(table, generator, version: "m/2"));
        await bumped.DeliverAsync(Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" })), CancellationToken.None);

        generator.Calls.ShouldBe(2);
    }

    [Test]
    public async Task Empty_text_stores_a_null_vector()
    {
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        await using var sink = new PgvectorSink("pgv", EmbedOptions(table, generator));
        await sink.InitializeAsync(CancellationToken.None);

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["title"] = "no name field" })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        generator.Calls.ShouldBe(0);
        var row = await RowAsync(table, "1");
        row!.Value.Vector.ShouldBeNull();
        row.Value.Hash.ShouldBeNull();
    }

    [Test]
    public async Task Deletes_remove_rows_and_last_write_wins_within_a_batch()
    {
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        await using var sink = new PgvectorSink("pgv", EmbedOptions(table, generator));
        await sink.InitializeAsync(CancellationToken.None);

        await sink.DeliverAsync(Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" })), CancellationToken.None);
        // An upsert then delete for the same id within one batch nets out to the delete.
        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "abc" }), Delete("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        (await RowAsync(table, "1")).ShouldBeNull();
    }

    [Test]
    public async Task Pass_through_mode_stores_the_provided_vector_and_strips_the_field()
    {
        var table = UniqueTable();
        await using var sink = new PgvectorSink("pgv", Options(table));
        await sink.InitializeAsync(CancellationToken.None);

        var result = await sink.DeliverAsync(Batch(Upsert("1", new WallabyDocument
        {
            ["name"] = "ab",
            ["embedding"] = new ReadOnlyMemory<float>([0.5f, -1f]),
        })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        var row = await RowAsync(table, "1");
        row!.Value.Vector.ShouldBe("[0.5,-1]");
        row.Value.Json.ShouldNotContain("embedding");
    }

    [Test]
    public async Task A_dimension_mismatch_fails_permanently()
    {
        var table = UniqueTable();
        await using var sink = new PgvectorSink("pgv", Options(table));
        await sink.InitializeAsync(CancellationToken.None);

        var result = await sink.DeliverAsync(Batch(Upsert("1", new WallabyDocument
        {
            ["embedding"] = new[] { 1f, 2f, 3f },
        })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("vector(2)");
    }

    [Test]
    public async Task A_transient_embedding_failure_is_retryable_and_the_retry_succeeds()
    {
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        generator.Failures.Enqueue(new HttpRequestException("429"));
        await using var sink = new PgvectorSink("pgv", EmbedOptions(table, generator));
        await sink.InitializeAsync(CancellationToken.None);
        var batch = Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" }));

        (await sink.DeliverAsync(batch, CancellationToken.None)).Status.ShouldBe(DeliveryStatus.RetryableFailure);
        (await sink.DeliverAsync(batch, CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task A_non_transient_embedding_failure_is_permanent()
    {
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        generator.Failures.Enqueue(new InvalidOperationException("bad api key"));
        var options = EmbedOptions(table, generator);
        options.IsTransientEmbeddingError = ex => ex is HttpRequestException;
        await using var sink = new PgvectorSink("pgv", options);
        await sink.InitializeAsync(CancellationToken.None);

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
    }

    [Test]
    public async Task A_record_without_destination_or_default_table_fails_permanently()
    {
        await using var sink = new PgvectorSink("pgv", Options(UniqueTable(), o => o.DefaultTable = null));

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" }, destination: null)), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("DefaultTable");
    }

    [Test]
    public async Task An_invalid_runtime_destination_fails_permanently()
    {
        await using var sink = new PgvectorSink("pgv", Options(UniqueTable()));

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new WallabyDocument(), destination: "bad\"; DROP TABLE x;--")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
    }

    [Test]
    public async Task Purge_empties_the_table_and_tolerates_a_missing_one()
    {
        var table = UniqueTable();
        var generator = new StubEmbeddingGenerator();
        await using var sink = new PgvectorSink("pgv", EmbedOptions(table, generator));
        await sink.InitializeAsync(CancellationToken.None);
        await sink.DeliverAsync(Batch(Upsert("1", new WallabyDocument { ["name"] = "ab" })), CancellationToken.None);

        await sink.PurgeAsync(new SinkPurgeRequest("public", "products", Destination: null), CancellationToken.None);
        (await RowAsync(table, "1")).ShouldBeNull();

        // A destination whose table was never created purges as a no-op.
        await sink.PurgeAsync(new SinkPurgeRequest("public", "products", $"never_{Guid.NewGuid():N}"), CancellationToken.None);
    }
}
