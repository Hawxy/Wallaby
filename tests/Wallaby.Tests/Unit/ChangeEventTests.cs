using Wallaby.Abstractions;

namespace Wallaby.Tests.Unit;

public class ChangeEventTests
{
    private sealed class Doc;

    private static ChangeEvent<Doc> Change(IReadOnlyList<object> primaryKey)
    {
        var meta = new ChangeMetadata("public", "docs", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, IsBackfill: false);
        return new ChangeEvent<Doc>(ChangeAction.Insert, meta, new Doc(), new Dictionary<string, object?>(), Changes: null, primaryKey);
    }

    [Test]
    public void GetPrimaryKey_casts_a_single_column_key()
    {
        Change([7]).GetPrimaryKey<int>().ShouldBe(7);
    }

    [Test]
    public void GetPrimaryKey_refuses_a_composite_key()
    {
        var ex = Should.Throw<InvalidOperationException>(() => Change([7, "a"]).GetPrimaryKey<int>());
        ex.Message.ShouldContain("public.docs");
        ex.Message.ShouldContain("2 key columns");
    }
}
