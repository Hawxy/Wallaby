using Wallaby.Sinks.Pgvector.Internal;

namespace Wallaby.Sinks.Pgvector.Tests.Unit;

public class FormatTests
{
    [Test]
    public void Text_hash_is_stable_and_version_sensitive()
    {
        PgvectorFormat.TextHash("m/1", "alpha").ShouldBe(PgvectorFormat.TextHash("m/1", "alpha"));
        PgvectorFormat.TextHash("m/1", "alpha").ShouldNotBe(PgvectorFormat.TextHash("m/2", "alpha"));
        PgvectorFormat.TextHash("m/1", "alpha").ShouldNotBe(PgvectorFormat.TextHash("m/1", "beta"));
    }

    [Test]
    public void Vectors_extract_from_memory_and_array_values()
    {
        PgvectorFormat.TryGetVector(new ReadOnlyMemory<float>([1f, 2f]), out var fromMemory).ShouldBeTrue();
        fromMemory.ToArray().ShouldBe([1f, 2f]);

        PgvectorFormat.TryGetVector(new[] { 3f }, out var fromArray).ShouldBeTrue();
        fromArray.ToArray().ShouldBe([3f]);

        PgvectorFormat.TryGetVector("not a vector", out _).ShouldBeFalse();
        PgvectorFormat.TryGetVector(new[] { 1.0 }, out _).ShouldBeFalse();
    }

    [Test]
    public void Identifiers_validate_to_safe_table_names()
    {
        PgvectorSink.IsValidIdentifier("products").ShouldBeTrue();
        PgvectorSink.IsValidIdentifier("tenant_42").ShouldBeTrue();
        PgvectorSink.IsValidIdentifier("").ShouldBeFalse();
        PgvectorSink.IsValidIdentifier("bad-name").ShouldBeFalse();
        PgvectorSink.IsValidIdentifier("x\"; DROP TABLE t;--").ShouldBeFalse();
        PgvectorSink.IsValidIdentifier(new string('x', 64)).ShouldBeFalse();
    }
}
