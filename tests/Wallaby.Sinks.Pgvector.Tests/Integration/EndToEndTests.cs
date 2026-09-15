using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Sinks.Pgvector.Tests.Integration.Infrastructure;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestModel;

namespace Wallaby.Sinks.Pgvector.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture, PgvectorFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class EndToEndTests(TestModelPostgresFixture source, PgvectorFixture destination)
{
    private async Task<string?> VectorAsync(string table, int id)
    {
        await using var cmd = destination.DataSource.CreateCommand(
            $"SELECT embedding::text FROM public.\"{table}\" WHERE id = $1");
        cmd.Parameters.Add(new NpgsqlParameter { Value = id.ToString() });
        return await cmd.ExecuteScalarAsync() as string;
    }

    [Test]
    public async Task Live_changes_embed_into_pgvector_and_a_re_backfill_reuses_stored_vectors()
    {
        var table = $"t_{Guid.NewGuid():N}";
        var generator = new StubEmbeddingGenerator();
        var sink = new PgvectorSink("pgv", new PgvectorSinkOptions
        {
            ConnectionString = destination.ConnectionString,
            Dimensions = 2,
            DefaultTable = table,
            EmbeddingGenerator = generator,
            EmbedText = d => (string?)d["name"],
            EmbeddingVersion = "m/1",
        });
        await sink.InitializeAsync(CancellationToken.None);

        await using var harness = WallabyTestHarness.ForTestModel(source.ConnectionString);
        harness.AddSink(sink)
            .Project<Product>("pgv", table, p => new WallabyDocument { ["name"] = p.Name },
                backfill: true, backfillVersion: "v1");
        await harness.SelfConfigureAsync();
        await harness.StartAsync();
        try
        {
            var categoryId = await harness.Db.AddCategoryAsync();
            var id = await harness.Db.AddProductAsync(categoryId, "alpha");

            await harness.WaitUntilAsync(async () => await VectorAsync(table, id) is not null);
            (await VectorAsync(table, id)).ShouldBe("[5,1]"); // the stub embeds "alpha" as [length, 1]
            var callsAfterLive = generator.Calls;

            // A backfill re-delivers every row; unchanged text is served from the destination's
            // stored hash, costing zero embedding calls.
            await harness.RunBackfillAsync(version: "v2");
            (await VectorAsync(table, id)).ShouldBe("[5,1]");
            generator.Calls.ShouldBe(callsAfterLive);
        }
        finally
        {
            await harness.StopAsync();
            await sink.DisposeAsync();
        }
    }
}
