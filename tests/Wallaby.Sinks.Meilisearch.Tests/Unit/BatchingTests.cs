using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using static Wallaby.Sinks.Meilisearch.Tests.Unit.MeilisearchTestHelpers;

namespace Wallaby.Sinks.Meilisearch.Tests.Unit;

/// <summary>
/// Large batches split into requests capped at <c>MaxRecordsPerRequest</c> records; the resulting tasks are
/// awaited by a single poll.
/// </summary>
public class BatchingTests
{
    [Test]
    public async Task Upserts_are_chunked_by_max_records_per_batch()
    {
        var stub = new StubHandler();
        var sink = Sink(stub, o => o.MaxRecordsPerRequest = 2);
        var records = Enumerable.Range(1, 5).Select(i => Upsert(i.ToString())).ToArray();

        var result = await sink.DeliverAsync(Batch(records), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        stub.Operations.ShouldBe(["add:2", "add:2", "add:1"]);
    }

    [Test]
    public async Task Deletions_are_chunked_by_max_records_per_batch()
    {
        var stub = new StubHandler();
        var sink = Sink(stub, o => o.MaxRecordsPerRequest = 2);
        var records = Enumerable.Range(1, 5).Select(i => Delete(i.ToString())).ToArray();

        var result = await sink.DeliverAsync(Batch(records), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        stub.Operations.ShouldBe(["delete:2", "delete:2", "delete:1"]);
    }

    [Test]
    public async Task Upserts_complete_before_deletions_within_an_index()
    {
        var stub = new StubHandler();
        var sink = Sink(stub, o => o.MaxRecordsPerRequest = 10);

        // Interleaved on input; the sink still applies all upserts, then all deletions.
        var result = await sink.DeliverAsync(
            Batch(Delete("1"), Upsert("2"), Delete("3"), Upsert("4")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        stub.Operations.ShouldBe(["add:2", "delete:2"]);
    }

    [Test]
    public async Task Every_task_across_indexes_is_enqueued_then_polled_together()
    {
        var stub = new StubHandler();
        var sink = Sink(stub, o => o.MaxRecordsPerRequest = 2);
        var records = Enumerable.Range(1, 3).Select(i => Upsert(i.ToString(), "products"))
            .Concat(Enumerable.Range(1, 3).Select(i => Upsert(i.ToString(), "orders")))
            .Append(Delete("9", "orders"))
            .ToArray();

        var result = await sink.DeliverAsync(Batch(records), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        var poll = stub.Requests.Single(r => r.Method == HttpMethod.Get);
        stub.Requests[^1].ShouldBeSameAs(poll); // every write was enqueued before the poll
        PolledUids(poll).Order().ShouldBe([1, 2, 3, 4, 5]);
    }

    [Test]
    public async Task Tasks_still_pending_at_the_wait_timeout_are_retried()
    {
        var stub = new StubHandler
        {
            Respond = (request, _) => request.Method == HttpMethod.Get
                ? Json(HttpStatusCode.OK, TaskResultJson(1, "processing"))
                : Json(HttpStatusCode.Accepted, TaskInfoJson(1)),
        };
        var sink = Sink(stub, o =>
        {
            o.WaitTimeout = TimeSpan.FromMilliseconds(100);
            o.WaitInterval = TimeSpan.FromMilliseconds(10);
        });

        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error!.ShouldContain("did not finish");
    }

    [Test]
    public void Non_positive_max_records_per_batch_is_rejected()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddMeilisearchSink("meili", o =>
        {
            o.Endpoint = "http://localhost:7700";
            o.MaxRecordsPerRequest = 0;
        }));
    }
}
