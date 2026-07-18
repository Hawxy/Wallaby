using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Sinks.Meilisearch.Tests.Integration.Infrastructure;

namespace Wallaby.Sinks.Meilisearch.Tests.Integration;

/// <summary>Sink-level <see cref="ISinkPurger"/> behaviour against a real Meilisearch.</summary>
[NotInParallel]
[ClassDataSource<MeilisearchFixture>(Shared = SharedType.PerTestSession)]
public class PurgeTests(MeilisearchFixture meili)
{
    private static string UniqueIndex() => $"purge_{Guid.NewGuid():N}";

    private MeilisearchSink Sink(string? defaultIndex = null) => TestMeilisearchSink.Create(
        "meili", new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey, DefaultIndex = defaultIndex });

    private static SinkRecord Upsert(string index, string id, string name)
        => new(index, id, new WallabyDocument { ["name"] = name }, IsDeletion: false,
            new ChangeMetadata("public", "products", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, false));

    [Test]
    public async Task Purge_empties_the_destination_index()
    {
        var index = UniqueIndex();
        var sink = Sink();
        var probe = new MeiliProbe(meili);

        var delivered = await sink.DeliverAsync(
            new SinkBatch("meili", [Upsert(index, "1", "alpha"), Upsert(index, "2", "beta")]), CancellationToken.None);
        delivered.Status.ShouldBe(DeliveryStatus.Success);
        (await probe.NameAsync(index, 1)).ShouldBe("alpha");

        await sink.PurgeAsync(new SinkPurgeRequest("public", "products", index), CancellationToken.None);

        (await probe.NameAsync(index, 1)).ShouldBeNull();
        (await probe.NameAsync(index, 2)).ShouldBeNull();
        // The index itself survives; only its documents are removed.
        (await probe.IndexExistsAsync(index)).ShouldBeTrue();
    }

    [Test]
    public async Task Purge_of_an_absent_index_is_a_no_op()
    {
        await Sink().PurgeAsync(new SinkPurgeRequest("public", "products", UniqueIndex()), CancellationToken.None);
    }

    [Test]
    public async Task Purge_with_no_destination_falls_back_to_the_default_index_or_fails()
    {
        var index = UniqueIndex();
        var sink = Sink(defaultIndex: index);
        var probe = new MeiliProbe(meili);

        (await sink.DeliverAsync(new SinkBatch("meili", [Upsert(index, "1", "alpha")]), CancellationToken.None))
            .Status.ShouldBe(DeliveryStatus.Success);
        await sink.PurgeAsync(new SinkPurgeRequest("public", "products", Destination: null), CancellationToken.None);
        (await probe.NameAsync(index, 1)).ShouldBeNull();

        await Should.ThrowAsync<WallabyConfigurationException>(() => Sink()
            .PurgeAsync(new SinkPurgeRequest("public", "products", Destination: null), CancellationToken.None));
    }
}
