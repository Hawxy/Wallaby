namespace Wallaby.Sinks.Pgvector.Tests.Unit;

public class RegistrationTests
{
    private static PgvectorSinkOptions Valid(Action<PgvectorSinkOptions>? mutate = null)
    {
        var options = new PgvectorSinkOptions
        {
            ConnectionString = "Host=localhost;Database=vectors;Username=u;Password=p",
            Dimensions = 3,
            DefaultTable = "documents",
        };
        mutate?.Invoke(options);
        return options;
    }

    [Test]
    public void Valid_options_pass()
    {
        PgvectorBuilderExtensions.Validate(Valid());
        PgvectorBuilderExtensions.Validate(Valid(o =>
        {
            o.EmbeddingGenerator = new StubEmbeddingGenerator();
            o.EmbedText = d => (string?)d["name"];
            o.EmbeddingVersion = "m/1";
        }));
    }

    [Test]
    public void Invalid_options_fail()
    {
        Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(Valid(o => o.ConnectionString = " ")));
        Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(Valid(o => o.Dimensions = 0)));
        Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(Valid(o => o.Schema = "bad-schema")));
        Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(Valid(o => o.DefaultTable = "bad.table")));
        Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(Valid(o => o.MaxRowsPerBatch = 0)));
        Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(Valid(o => o.MaxEmbeddingBatchSize = 0)));
        Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(Valid(o => o.MaxEmbeddingConcurrency = 0)));
        Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(Valid(o => o.VectorField = " ")));
    }

    [Test]
    public void Direct_construction_validates_options()
    {
        // Schema and DefaultTable reach interpolated SQL, so the constructor enforces the identifier
        // rule even when the builder's Validate is bypassed.
        Should.Throw<ArgumentException>(() => new PgvectorSink("pgv", Valid(o => o.Schema = "bad\"schema")));
        Should.Throw<ArgumentException>(() => new PgvectorSink("pgv", Valid(o => o.DefaultTable = "bad.table")));
    }

    [Test]
    public void A_partial_embedding_configuration_fails()
    {
        var ex = Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(
            Valid(o => o.EmbeddingGenerator = new StubEmbeddingGenerator())));
        ex.Message.ShouldContain("together");

        Should.Throw<ArgumentException>(() => PgvectorBuilderExtensions.Validate(
            Valid(o => { o.EmbedText = d => "x"; o.EmbeddingVersion = "m/1"; })));
    }
}
