using System.Text;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Wallaby.Abstractions;
using static Wallaby.Sinks.Elasticsearch.Tests.Unit.SinkTestHelpers;

namespace Wallaby.Sinks.Elasticsearch.Tests.Unit;

/// <summary>Invoker that returns a canned response while recording every bulk body and URL.</summary>
internal sealed class CapturingInvoker(string body, int statusCode = 200, Exception? exception = null) : IRequestInvoker
{
    private readonly InMemoryRequestInvoker _inner = new(Encoding.UTF8.GetBytes(body), statusCode, exception);

    public List<string> Payloads { get; } = [];
    public List<string> Urls { get; } = [];

    public ResponseFactory ResponseFactory => _inner.ResponseFactory;

    public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
        where TResponse : TransportResponse, new()
        => _inner.Request<TResponse>(endpoint, boundConfiguration, postData);

    public Task<TResponse> RequestAsync<TResponse>(
        Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
        where TResponse : TransportResponse, new()
    {
        Urls.Add(endpoint.PathAndQuery);
        if (postData is not null)
        {
            using var buffer = new MemoryStream();
            postData.Write(buffer, boundConfiguration.ConnectionSettings, false);
            Payloads.Add(Encoding.UTF8.GetString(buffer.ToArray()));
        }
        return _inner.RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken);
    }

    public void Dispose() { }
}

/// <summary>Delivery classification and request shaping of <see cref="ElasticsearchSink"/>.</summary>
public class DeliveryTests
{
    private const string AllOk = """{"took":1,"errors":false,"items":[{"index":{"_index":"products","_id":"1","status":201}}]}""";

    private static ElasticsearchSink Sink(IRequestInvoker invoker, Action<ElasticsearchSinkOptions>? configure = null)
    {
        var options = new ElasticsearchSinkOptions
        {
            Endpoint = "http://elasticsearch.local:9200",
            ConfigureConnection = uri => new ElasticsearchClientSettings(new SingleNodePool(uri), invoker),
        };
        configure?.Invoke(options);
        return new ElasticsearchSink(SinkName, options);
    }

    [Test]
    public async Task Successful_bulk_returns_success()
    {
        var invoker = new CapturingInvoker(AllOk);
        using var sink = Sink(invoker);

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?> { ["name"] = "alpha" })), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        invoker.Urls.Single().ShouldStartWith("/_bulk");
        invoker.Payloads.Single().ShouldContain("\"_id\":\"1\"");
    }

    [Test]
    public async Task Large_batches_are_split_into_sequential_requests()
    {
        var invoker = new CapturingInvoker(AllOk);
        using var sink = Sink(invoker, o => o.MaxActionsPerRequest = 2);

        var result = await sink.DeliverAsync(Batch(
            Upsert("1", new Dictionary<string, object?>()),
            Upsert("2", new Dictionary<string, object?>()),
            Upsert("3", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        invoker.Payloads.Count.ShouldBe(2);
        invoker.Payloads[0].ShouldContain("\"_id\":\"2\"");
        invoker.Payloads[0].ShouldNotContain("\"_id\":\"3\"");
        invoker.Payloads[1].ShouldContain("\"_id\":\"3\"");
    }

    [Test]
    public async Task Refresh_option_requests_wait_for()
    {
        var invoker = new CapturingInvoker(AllOk);
        using var sink = Sink(invoker, o => o.Refresh = true);

        await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        invoker.Urls.Single().ShouldContain("refresh=wait_for");
    }

    [Test]
    [Arguments(408)]
    [Arguments(429)]
    [Arguments(503)]
    public async Task Throttling_and_server_errors_are_retryable(int status)
    {
        using var sink = Sink(new CapturingInvoker("""{"error":"unavailable"}""", status));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error!.ShouldContain(status.ToString());
    }

    [Test]
    public async Task Request_level_rejection_is_permanent()
    {
        using var sink = Sink(new CapturingInvoker("""{"error":"malformed"}""", 400));

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
        using var sink = Sink(new CapturingInvoker(body));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("mapper_parsing_exception");
        result.Error!.ShouldContain("'2'");
    }

    [Test]
    public async Task Deleting_an_absent_document_is_success()
    {
        const string body = """{"took":1,"errors":true,"items":[{"delete":{"_index":"products","_id":"9","status":404,"result":"not_found"}}]}""";
        using var sink = Sink(new CapturingInvoker(body));

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
        using var sink = Sink(new CapturingInvoker(body));

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
        using var sink = Sink(new CapturingInvoker(body));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
    }

    [Test]
    public async Task Transport_failure_is_retryable()
    {
        using var sink = Sink(new CapturingInvoker("", exception: new HttpRequestException("connection refused")));

        var result = await sink.DeliverAsync(Batch(Upsert("1", new Dictionary<string, object?>())), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error!.ShouldContain("connection refused");
    }

    [Test]
    public async Task Record_without_a_resolvable_index_fails_permanently_before_any_request()
    {
        var invoker = new CapturingInvoker(AllOk);
        using var sink = Sink(invoker);

        var result = await sink.DeliverAsync(
            Batch(Upsert("1", new Dictionary<string, object?>(), destination: null)), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("DefaultIndex");
        invoker.Payloads.ShouldBeEmpty();
    }
}
