using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Reselect healing end to end: under REPLICA IDENTITY DEFAULT an update leaving a TOASTed column
/// unchanged omits its value from the wire; the pipeline re-reads the row and delivers the complete
/// document instead of halting. A vanished row's change is dropped and the stream advances. With
/// healing disabled, the change is a poison change that faults the pipeline.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class ToastReselectHealingTests(TestModelPostgresFixture pg)
{
    // Base64 of seeded random bytes: incompressible, so it TOASTs well past the ~2KB threshold.
    private static string LargePayload()
    {
        var bytes = new byte[16_000];
        new Random(42).NextBytes(bytes);
        return Convert.ToBase64String(bytes);
    }

    private static string? NameOf(SinkRecord r) => r.Document?.GetValueOrDefault("Name") as string;

    [Test]
    public async Task An_unavailable_toast_value_heals_by_reselect()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var capture = harness.AddCaptureSink();
        using var reselected = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.changes.reselected");
        await harness.SelfConfigureAsync();

        // The session database is shared; another test may have left products on FULL.
        await harness.Db.SetReplicaIdentityDefaultAsync("products");
        var payload = LargePayload();
        var categoryId = await harness.Db.AddCategoryAsync();
        var productId = await harness.Db.AddProductAsync(categoryId, "toasty");
        await harness.Db.SetProductDescriptionAsync(productId, payload);
        // Changes only Name: the unchanged TOASTed Description is not on the wire under DEFAULT identity.
        await harness.Db.UpdateProductNameAsync(productId, "toasty-renamed");

        await harness.RunUntilAsync(() => capture.For("products").Any(r =>
            NameOf(r) == "toasty-renamed" &&
            r.Document?.GetValueOrDefault(nameof(Product.Description)) as string == payload));

        var healed = reselected.GetMeasurementSnapshot().Where(m =>
            Equals(m.Tags.GetValueOrDefault("wallaby.reselect.outcome"), "healed")).ToList();
        healed.Sum(m => m.Value).ShouldBe(1);
        healed.ShouldAllBe(m => Equals(m.Tags.GetValueOrDefault("wallaby.table"), "public.products"));
    }

    [Test]
    public async Task A_vanished_row_drops_the_change_and_the_stream_advances()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var capture = harness.AddCaptureSink();
        using var reselected = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.changes.reselected");
        await harness.SelfConfigureAsync();

        await harness.Db.SetReplicaIdentityDefaultAsync("products");
        var categoryId = await harness.Db.AddCategoryAsync();
        var productId = await harness.Db.AddProductAsync(categoryId, "doomed");
        await harness.Db.SetProductDescriptionAsync(productId, LargePayload());
        // By the time the pipeline streams this update, the row is gone: the reselect finds nothing.
        await harness.Db.UpdateProductNameAsync(productId, "doomed-renamed");
        await harness.Db.DeleteProductAsync(productId);

        await harness.RunUntilAsync(() =>
        {
            var deletion = capture.For("products").FirstOrDefault(r => r.IsDeletion);
            return deletion is not null && harness.LastAcknowledgedLsn >= deletion.Metadata.CommitLsn;
        });

        capture.For("products").ShouldNotContain(r => NameOf(r) == "doomed-renamed");
        reselected.GetMeasurementSnapshot()
            .Where(m => Equals(m.Tags.GetValueOrDefault("wallaby.reselect.outcome"), "row_gone"))
            .Sum(m => m.Value).ShouldBe(1);
    }

    [Test]
    public async Task With_healing_disabled_the_change_is_a_poison_change()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        harness.ReselectUnavailableValues = false;
        var capture = harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        await harness.Db.SetReplicaIdentityDefaultAsync("products");
        var categoryId = await harness.Db.AddCategoryAsync();
        var productId = await harness.Db.AddProductAsync(categoryId, "poison");
        await harness.Db.SetProductDescriptionAsync(productId, LargePayload());
        await harness.Db.UpdateProductNameAsync(productId, "poison-renamed");

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await harness.RunUntilAsync(
                () => capture.For("products").Any(r => NameOf(r) == "poison-renamed"),
                TimeSpan.FromSeconds(30)));

        ex.GetBaseException().Message.ShouldContain("was not carried in the change");
    }
}
