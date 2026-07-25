using System.Net;
using Meilisearch;
using Wallaby.Abstractions;
using static Wallaby.Sinks.Meilisearch.Tests.Unit.MeilisearchTestHelpers;

namespace Wallaby.Sinks.Meilisearch.Tests.Unit;

/// <summary>
/// Delivery failures classify by Meilisearch error code: deterministic configuration/credential/payload
/// errors fail permanently (halting the pipeline); everything environment-fixable stays retryable.
/// </summary>
public class ClassificationTests
{
    [Test]
    [Arguments("invalid_api_key")]
    [Arguments("missing_authorization_header")]
    [Arguments("payload_too_large")]
    [Arguments("invalid_document_id")]
    [Arguments("missing_document_id")]
    [Arguments("invalid_document_fields")]
    [Arguments("invalid_document_geo_field")]
    [Arguments("invalid_index_uid")]
    [Arguments("invalid_index_primary_key")]
    [Arguments("index_primary_key_already_exists")]
    [Arguments("index_primary_key_multiple_candidates_found")]
    [Arguments("bad_request")]
    public async Task Api_error_with_a_permanent_code_fails_permanently(string code)
    {
        var stub = new StubHandler { Respond = (_, _) => Json(HttpStatusCode.BadRequest, ApiErrorJson(code)) };
        var sink = Sink(stub);

        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain(code);
    }

    [Test]
    [Arguments("index_not_found")] // on the upsert path only; the delete path swallows it (see AttributeValidationTests)
    [Arguments("internal")]
    [Arguments("no_space_left_on_device")]
    public async Task Api_error_with_an_environment_fixable_code_is_retried(string code)
    {
        var stub = new StubHandler { Respond = (_, _) => Json(HttpStatusCode.ServiceUnavailable, ApiErrorJson(code)) };
        var sink = Sink(stub);

        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
    }

    [Test]
    public async Task Error_response_without_a_meilisearch_body_is_retried()
    {
        // A proxy/load-balancer 503 has no Meilisearch error code to classify on.
        var stub = new StubHandler
        {
            Respond = (_, _) => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
            {
                Content = new StringContent("upstream unavailable"),
            },
        };
        var sink = Sink(stub);

        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
    }

    private static StubHandler TaskFailureStub(string status, string? errorCode)
        => new()
        {
            Respond = (request, _) => request.Method == HttpMethod.Get
                ? Json(HttpStatusCode.OK, TaskResultJson(1, status, errorCode))
                : Json(HttpStatusCode.Accepted, TaskInfoJson(1)),
        };

    [Test]
    public async Task Task_failure_with_a_permanent_code_fails_permanently()
    {
        var sink = Sink(TaskFailureStub("failed", "invalid_document_fields"));

        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("invalid_document_fields");
    }

    [Test]
    public async Task Task_failure_with_a_transient_code_is_retried()
    {
        var sink = Sink(TaskFailureStub("failed", "internal"));

        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
    }

    [Test]
    public async Task Task_failure_without_detail_is_retried()
    {
        var sink = Sink(TaskFailureStub("failed", errorCode: null));

        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error!.ShouldContain("no detail");
    }

    [Test]
    public async Task Transport_failure_is_retried()
    {
        var stub = new StubHandler { Throw = new HttpRequestException("connection refused") };
        var sink = Sink(stub);

        var result = await sink.DeliverAsync(Batch(Upsert("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
    }

    [Test]
    public async Task Record_without_destination_or_default_index_fails_permanently()
    {
        var stub = new StubHandler();
        var sink = Sink(stub); // no DefaultIndex configured

        var result = await sink.DeliverAsync(Batch(Upsert("1", destination: null)), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("DefaultIndex");
        stub.Requests.ShouldBeEmpty(); // fails before any network call
    }

    [Test]
    public async Task Initialization_auth_failure_is_not_treated_as_missing_index()
    {
        var stub = new StubHandler
        {
            Respond = (_, _) => Json(HttpStatusCode.Forbidden, ApiErrorJson("invalid_api_key")),
        };
        var sink = Sink(stub, o => o.ConfigureIndex("products"));

        await Should.ThrowAsync<MeilisearchApiError>(() => sink.InitializeAsync(CancellationToken.None));

        // The auth failure must not fall through to a create attempt against a possibly-existing index.
        stub.Requests.Count.ShouldBe(1);
    }

    [Test]
    public async Task Cancellation_is_rethrown_not_classified()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var sink = Sink(new StubHandler());

        await Should.ThrowAsync<OperationCanceledException>(
            () => sink.DeliverAsync(Batch(Upsert("1")), cts.Token));
    }
}
