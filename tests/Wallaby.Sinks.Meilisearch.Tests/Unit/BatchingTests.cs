using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using static Wallaby.Sinks.Meilisearch.Tests.Unit.MeilisearchTestHelpers;

namespace Wallaby.Sinks.Meilisearch.Tests.Unit;

/// <summary>Large batches split into sequential requests capped at <c>MaxRecordsPerBatch</c> records.</summary>
public class BatchingTests
{
    [Test]
    public async Task Upserts_are_chunked_by_max_records_per_batch()
    {
        var stub = new StubHandler();
        var sink = Sink(stub, o => o.MaxRecordsPerBatch = 2);
        var records = Enumerable.Range(1, 5).Select(i => Upsert(i.ToString())).ToArray();

        var result = await sink.DeliverAsync(Batch(records), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        stub.Operations.ShouldBe(["add:2", "add:2", "add:1"]);
    }

    [Test]
    public async Task Deletions_are_chunked_by_max_records_per_batch()
    {
        var stub = new StubHandler();
        var sink = Sink(stub, o => o.MaxRecordsPerBatch = 2);
        var records = Enumerable.Range(1, 5).Select(i => Delete(i.ToString())).ToArray();

        var result = await sink.DeliverAsync(Batch(records), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        stub.Operations.ShouldBe(["delete:2", "delete:2", "delete:1"]);
    }

    [Test]
    public async Task Upserts_complete_before_deletions_within_an_index()
    {
        var stub = new StubHandler();
        var sink = Sink(stub, o => o.MaxRecordsPerBatch = 10);

        // Interleaved on input; the sink still applies all upserts, then all deletions.
        var result = await sink.DeliverAsync(
            Batch(Delete("1"), Upsert("2"), Delete("3"), Upsert("4")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        stub.Operations.ShouldBe(["add:2", "delete:2"]);
    }

    [Test]
    public void Non_positive_max_records_per_batch_is_rejected()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Host = "http://localhost:7700";
            o.MaxRecordsPerBatch = 0;
        }));
    }
}
