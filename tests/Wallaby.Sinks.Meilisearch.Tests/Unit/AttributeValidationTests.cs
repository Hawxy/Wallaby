using System.Net;
using Wallaby.Abstractions;
using Wallaby.Sinks.Meilisearch;
using static Wallaby.Sinks.Meilisearch.Tests.Unit.MeilisearchTestHelpers;

namespace Wallaby.Sinks.Meilisearch.Tests.Unit;

/// <summary>
/// Configured-attribute validation resolves a dotted attribute the way Meilisearch does — a literal key
/// first, then segment-by-segment through nested dictionaries and array elements. Only a dictionary
/// provably lacking a key is reported; any shape validation can't inspect passes, because a false
/// positive here is a permanent failure that halts the pipeline.
/// </summary>
public class AttributeValidationTests
{
    private static MeilisearchSink SinkRequiring(StubHandler stub, params string[] attributes)
        => Sink(stub, o => o.ConfigureIndex("products", s => s.FilterableAttributes = [.. attributes]));

    private static SinkRecord UpsertWith(WallabyDocument document)
        => new("products", "1", document, IsDeletion: false, Meta());

    private static async Task<DeliveryResult> DeliverAsync(MeilisearchSink sink, WallabyDocument document)
        => await sink.DeliverAsync(Batch(UpsertWith(document)), CancellationToken.None);

    [Test]
    public async Task A_nested_dictionary_satisfies_a_dotted_attribute()
    {
        var sink = SinkRequiring(new StubHandler(), "author.name");

        var result = await DeliverAsync(sink, new WallabyDocument
        {
            ["author"] = new WallabyDocument { ["name"] = "kanga" },
        });

        result.Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task A_poco_value_passes_a_dotted_attribute_unchecked()
    {
        var sink = SinkRequiring(new StubHandler(), "author.name");

        // Anonymous types (and POCOs) serialize with their properties, but validation can't see them.
        var result = await DeliverAsync(sink, new WallabyDocument { ["author"] = new { name = "kanga" } });

        result.Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task An_array_of_objects_satisfies_a_dotted_attribute()
    {
        var sink = SinkRequiring(new StubHandler(), "authors.name");

        var result = await DeliverAsync(sink, new WallabyDocument
        {
            ["authors"] = new object[]
            {
                new WallabyDocument { ["bio"] = "no name here" },
                new WallabyDocument { ["name"] = "kanga" },
            },
        });

        result.Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task An_empty_array_passes_validation()
    {
        var sink = SinkRequiring(new StubHandler(), "authors.name");

        var result = await DeliverAsync(sink, new WallabyDocument { ["authors"] = Array.Empty<object>() });

        result.Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task A_literal_key_containing_a_dot_satisfies_the_attribute()
    {
        var sink = SinkRequiring(new StubHandler(), "author.name");

        var result = await DeliverAsync(sink, new WallabyDocument { ["author.name"] = "kanga" });

        result.Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task A_null_leaf_counts_as_present()
    {
        var sink = SinkRequiring(new StubHandler(), "author.name");

        var result = await DeliverAsync(sink, new WallabyDocument
        {
            ["author"] = new WallabyDocument { ["name"] = null },
        });

        result.Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task A_nested_dictionary_provably_lacking_the_key_fails_permanently()
    {
        var stub = new StubHandler();
        var sink = SinkRequiring(stub, "author.name");

        var result = await DeliverAsync(sink, new WallabyDocument
        {
            ["author"] = new WallabyDocument { ["bio"] = "kanga" },
        });

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("author.name");
        stub.Requests.ShouldBeEmpty(); // fails before any network call
    }

    [Test]
    public async Task A_flat_attribute_missing_from_the_document_still_fails_permanently()
    {
        var sink = SinkRequiring(new StubHandler(), "category");

        var result = await DeliverAsync(sink, new WallabyDocument { ["name"] = "kanga" });

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("category");
    }

    [Test]
    public async Task A_delete_only_batch_to_a_missing_index_succeeds()
    {
        // Deletes don't auto-create indexes, so a scoped index that only ever saw deletions would
        // otherwise retry (and halt) forever.
        var stub = new StubHandler
        {
            Respond = (request, _) => request.RequestUri!.AbsolutePath.EndsWith("/documents/delete-batch", StringComparison.Ordinal)
                ? Json(HttpStatusCode.NotFound, ApiErrorJson("index_not_found"))
                : Json(HttpStatusCode.OK, TaskResultJson(1, "succeeded")),
        };
        var sink = Sink(stub);

        var result = await sink.DeliverAsync(Batch(Delete("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task A_delete_whose_task_fails_with_a_missing_index_succeeds()
    {
        // The 404 can also surface asynchronously, from the enqueued task instead of the request.
        var stub = new StubHandler
        {
            Respond = (request, _) => request.Method == HttpMethod.Get
                ? Json(HttpStatusCode.OK, TaskResultJson(1, "failed", "index_not_found"))
                : Json(HttpStatusCode.Accepted, TaskInfoJson(1)),
        };
        var sink = Sink(stub);

        var result = await sink.DeliverAsync(Batch(Delete("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task Upserts_in_the_same_batch_still_deliver_when_the_deletions_index_is_missing()
    {
        var stub = new StubHandler
        {
            Respond = (request, body) => request.RequestUri!.AbsolutePath.EndsWith("/documents/delete-batch", StringComparison.Ordinal)
                ? Json(HttpStatusCode.NotFound, ApiErrorJson("index_not_found"))
                : new MeiliSimulator().Respond(request, body),
        };
        var sink = Sink(stub);

        var result = await sink.DeliverAsync(
            Batch(Upsert("1"), Delete("2", destination: "ghost")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
    }
}
