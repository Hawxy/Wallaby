using Wallaby.Abstractions;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;

namespace Wallaby.EntityFrameworkCore.IntegrationTests;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class PipelineTests(TestModelPostgresFixture pg)
{
    private static string? NameOf(SinkRecord r) => r.Document?.GetValueOrDefault("Name") as string;

    private static List<string> ProductNames(CaptureSink capture)
        => capture.For("products").Select(NameOf).Where(n => n is not null).Select(n => n!).ToList();

    [Test]
    public async Task Changes_are_delivered_to_sink_in_commit_order()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast();
        var capture = harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "alpha");
        await harness.Db.AddProductAsync(categoryId, "beta");

        await harness.RunUntilAsync(() => capture.For("products").Count(r => r.Document is not null) >= 2);

        var names = capture.For("products").Select(NameOf).Where(n => n is "alpha" or "beta").Select(n => n!).ToList();
        names.ShouldBe(new[] { "alpha", "beta" }, ignoreOrder: true);
    }

    [Test]
    public async Task Restart_resumes_from_confirmed_flush_lsn_without_loss_or_duplication()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast();
        var capture = harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "A");

        // Run 1: consume "A" and ensure its commit was acknowledged before stopping.
        await harness.RunUntilAsync(() =>
        {
            var a = capture.For("products").FirstOrDefault(r => NameOf(r) == "A");
            return a is not null && harness.LastAcknowledgedLsn >= a.Metadata.CommitLsn;
        });
        ProductNames(capture).ShouldContain("A");

        // Run 2: a fresh stream on the same slot must resume after "A".
        capture.Clear();
        await harness.Db.AddProductAsync(categoryId, "B");
        await harness.RunUntilAsync(() => capture.For("products").Any(r => NameOf(r) == "B"));

        var run2 = ProductNames(capture);
        run2.ShouldContain("B");
        run2.ShouldNotContain("A"); // no re-delivery
    }
}
