using System.Text;
using OpenSearch.Client;
using OpenSearch.Net;
using Wallaby.Abstractions;
using static Wallaby.Sinks.OpenSearch.Tests.Unit.SinkTestHelpers;

namespace Wallaby.Sinks.OpenSearch.Tests.Unit;

/// <summary>Connection that returns a canned response while recording every bulk body and URL.</summary>
internal sealed class CapturingConnection(string body, int statusCode = 200, Exception? exception = null)
    : InMemoryConnection(Encoding.UTF8.GetBytes(body), statusCode, exception)
{
    public List<string> Payloads { get; } = [];
    public List<string> Urls { get; } = [];

    public override Task<TResponse> RequestAsync<TResponse>(RequestData requestData, CancellationToken cancellationToken)
    {
        Urls.Add(requestData.Uri.PathAndQuery);
        using var buffer = new MemoryStream();
        requestData.PostData.Write(buffer, requestData.ConnectionSettings);
        Payloads.Add(Encoding.UTF8.GetString(buffer.ToArray()));
        return base.RequestAsync<TResponse>(requestData, cancellationToken);
    }
}

/// <summary>Delivery classification and request shaping of <see cref="OpenSearchSink"/>.</summary>
public class DeliveryTests
{
    private const string AllOk = """{"took":1,"errors":false,"items":[{"index":{"_index":"products","_id":"1","status":201}}]}""";

    private static OpenSearchSink Sink(IConnection connection, Action<OpenSearchSinkOptions>? configure = null)
    {
        var options = new OpenSearchSinkOptions
        {
            Endpoint = "http://opensearch.local:9200",
            ConfigureConnection = uri => new ConnectionSettings(new SingleNodeConnectionPool(uri), connection),
        };
        configure?.Invoke(options);
        return new OpenSearchSink(SinkName, options);
    }

    [Test]
    public async Task Successful_bulk_returns_success()
    {
        var connection = new CapturingConnection(AllOk);
        using var sink = Sink(connection);

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?> { ["name"] = "alpha" })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        connection.Urls.Single().ShouldStartWith("/_bulk");
        connection.Payloads.Single().ShouldContain("\"_id\":\"1\"");
    }

    [Test]
    public async Task Large_batches_are_split_into_sequential_requests()
    {
        var connection = new CapturingConnection(AllOk);
        using var sink = Sink(connection, o => o.MaxActionsPerRequest = 2);

        var result = await sink.DeliverAsync(Batch(
            Upsert("1", new Dictionary<string, object?>()),
            Upsert("2", new Dictionary<string, object?>()),
            Upsert("3", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        connection.Payloads.Count.ShouldBe(2);
        connection.Payloads[0].ShouldContain("\"_id\":\"2\"");
        connection.Payloads[0].ShouldNotContain("\"_id\":\"3\"");
        connection.Payloads[1].ShouldContain("\"_id\":\"3\"");
    }

    [Test]
    public async Task Refresh_option_requests_wait_for()
    {
        var connection = new CapturingConnection(AllOk);
        using var sink = Sink(connection, o => o.Refresh = true);

        await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        connection.Urls.Single().ShouldContain("refresh=wait_for");
    }

    [Test]
    [Arguments(408)]
    [Arguments(429)]
    [Arguments(503)]
    public async Task Throttling_and_server_errors_are_retryable(int status)
    {
        using var sink = Sink(new CapturingConnection("""{"error":"unavailable"}""", status));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error!.ShouldContain(status.ToString());
    }

    [Test]
    public async Task Request_level_rejection_is_permanent()
    {
        using var sink = Sink(new CapturingConnection("""{"error":"malformed"}""", 400));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
    }

    [Test]
    public async Task Item_level_mapping_rejection_is_permanent_with_detail()
    {
        const string body = """
            {"took":1,"errors":true,"items":[
              {"index":{"_index":"products","_id":"1","status":201}},
              {"index":{"_index":"products","_id":"2","status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [price]"}}}]}
            """;
        using var sink = Sink(new CapturingConnection(body));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("mapper_parsing_exception");
        result.Error!.ShouldContain("'2'");
    }

    [Test]
    public async Task Deleting_an_absent_document_is_success()
    {
        const string body = """{"took":1,"errors":true,"items":[{"delete":{"_index":"products","_id":"9","status":404,"result":"not_found"}}]}""";
        using var sink = Sink(new CapturingConnection(body));

        var result = await sink.DeliverAsync(Batch(Delete("9")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
    }

    [Test]
    public async Task Item_level_throttling_is_retryable()
    {
        const string body = """
            {"took":1,"errors":true,"items":[
              {"index":{"_index":"products","_id":"1","status":429,"error":{"type":"circuit_breaking_exception","reason":"too much load"}}}]}
            """;
        using var sink = Sink(new CapturingConnection(body));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
    }

    [Test]
    public async Task A_permanent_item_outweighs_retryable_items()
    {
        const string body = """
            {"took":1,"errors":true,"items":[
              {"index":{"_index":"products","_id":"1","status":429}},
              {"index":{"_index":"products","_id":"2","status":400,"error":{"type":"mapper_parsing_exception","reason":"bad"}}}]}
            """;
        using var sink = Sink(new CapturingConnection(body));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
    }

    [Test]
    [Arguments("not json")]
    [Arguments("")]
    public async Task Unrecognized_or_empty_bulk_response_is_retryable(string body)
    {
        using var sink = Sink(new CapturingConnection(body));

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
    }

    [Test]
    public async Task Transport_failure_is_retryable()
    {
        using var sink = Sink(new CapturingConnection("", exception: new HttpRequestException("connection refused")));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error!.ShouldContain("connection refused");
    }

    [Test]
    public async Task Record_without_a_resolvable_index_fails_permanently_before_any_request()
    {
        var connection = new CapturingConnection(AllOk);
        using var sink = Sink(connection);

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>(), destination: null)), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("DefaultIndex");
        connection.Payloads.ShouldBeEmpty();
    }
}
