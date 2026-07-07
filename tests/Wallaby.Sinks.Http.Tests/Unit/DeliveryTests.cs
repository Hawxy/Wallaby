using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using static Wallaby.Sinks.Http.Tests.Unit.SinkTestHelpers;

namespace Wallaby.Sinks.Http.Tests.Unit;

/// <summary>Chunking, outcome classification, signing, and named-client behaviour of <see cref="HttpSink"/>.</summary>
public class DeliveryTests
{
    private static SinkRecord[] Upserts(params string[] ids)
        => ids.Select(id => Upsert(id, new Dictionary<string, object?> { ["id"] = id })).ToArray();

    private static IReadOnlyList<string> RecordIds(byte[] body)
    {
        using var envelope = JsonDocument.Parse(body);
        return envelope.RootElement.GetProperty("records").EnumerateArray()
            .Select(r => r.GetProperty("id").GetString()!).ToList();
    }

    [Test]
    public async Task Large_batches_are_chunked_sequentially_in_order()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler, o => o.MaxRecordsPerRequest = 2);

        var result = await sink.DeliverAsync(Batch(Upserts("1", "2", "3", "4", "5")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        handler.Requests.Count.ShouldBe(3);
        RecordIds(handler.Requests[0].Body).ShouldBe(["1", "2"]);
        RecordIds(handler.Requests[1].Body).ShouldBe(["3", "4"]);
        RecordIds(handler.Requests[2].Body).ShouldBe(["5"]);
    }

    [Test]
    public async Task A_failing_chunk_stops_the_delivery()
    {
        var handler = new CapturingHandler
        {
            Respond = i => new HttpResponseMessage(i == 1 ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.OK),
        };
        var sink = CreateSink(handler, o => o.MaxRecordsPerRequest = 1);

        var result = await sink.DeliverAsync(Batch(Upserts("1", "2", "3")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        handler.Requests.Count.ShouldBe(2);
    }

    [Test]
    [Arguments(HttpStatusCode.RequestTimeout)]
    [Arguments(HttpStatusCode.TooManyRequests)]
    [Arguments(HttpStatusCode.InternalServerError)]
    [Arguments(HttpStatusCode.ServiceUnavailable)]
    public async Task Transient_statuses_are_retryable(HttpStatusCode status)
    {
        var handler = new CapturingHandler { Respond = _ => new HttpResponseMessage(status) };
        var sink = CreateSink(handler);

        var result = await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error.ShouldNotBeNull().ShouldContain(((int)status).ToString());
    }

    [Test]
    [Arguments(HttpStatusCode.BadRequest)]
    [Arguments(HttpStatusCode.NotFound)]
    [Arguments(HttpStatusCode.UnprocessableEntity)]
    public async Task Rejections_are_permanent(HttpStatusCode status)
    {
        var handler = new CapturingHandler { Respond = _ => new HttpResponseMessage(status) };
        var sink = CreateSink(handler);

        var result = await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
    }

    [Test]
    public async Task Network_failures_are_retryable()
    {
        var handler = new CapturingHandler { Throw = new HttpRequestException("connection refused") };
        var sink = CreateSink(handler);

        var result = await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
    }

    [Test]
    public async Task Request_timeout_is_retryable()
    {
        var handler = new CapturingHandler { OnRequest = ct => Task.Delay(10_000, ct) };
        var sink = CreateSink(handler, o => o.TimeoutMs = 100);

        var result = await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.RetryableFailure);
        result.Error.ShouldNotBeNull().ShouldContain("timed out");
    }

    [Test]
    public async Task Cancellation_propagates()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Should.ThrowAsync<OperationCanceledException>(
            () => sink.DeliverAsync(Batch(Upserts("1")), cts.Token));
    }

    [Test]
    public async Task Body_is_signed_when_a_secret_is_configured()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler, o => o.SigningSecret = "s3cret");

        (await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        var captured = handler.Requests.ShouldHaveSingleItem();
        var expected = Convert.ToHexStringLower(HMACSHA256.HashData(Encoding.UTF8.GetBytes("s3cret"), captured.Body));
        captured.Signature.ShouldBe($"sha256={expected}");
    }

    [Test]
    public async Task Requests_are_unsigned_without_a_secret()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler);

        (await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        handler.Requests.ShouldHaveSingleItem().Signature.ShouldBeNull();
    }

    [Test]
    public async Task Named_client_message_handlers_participate_in_delivery()
    {
        var handler = new CapturingHandler();
        var services = new ServiceCollection();
        services.AddTransient<HeaderStampingHandler>();
        services.AddHttpClient(HttpSink.ClientNameFor(SinkName))
            .AddHttpMessageHandler<HeaderStampingHandler>()
            .ConfigurePrimaryHttpMessageHandler(() => handler);
        var factory = services.BuildServiceProvider().GetRequiredService<IHttpClientFactory>();

        var sink = new HttpSink(SinkName, new HttpSinkOptions { Endpoint = "https://receiver.example/hooks" }, factory);
        (await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        handler.Requests.ShouldHaveSingleItem().Request.Headers
            .GetValues("X-Stamped").Single().ShouldBe("yes");
    }

    [Test]
    public async Task Explicit_client_name_overrides_the_convention()
    {
        var handler = new CapturingHandler();
        var services = new ServiceCollection();
        services.AddHttpClient("custom").ConfigurePrimaryHttpMessageHandler(() => handler);
        var factory = services.BuildServiceProvider().GetRequiredService<IHttpClientFactory>();

        var options = new HttpSinkOptions { Endpoint = "https://receiver.example/hooks", HttpClientName = "custom" };
        var sink = new HttpSink(SinkName, options, factory);

        (await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);
        handler.Requests.ShouldHaveSingleItem();
    }

    private sealed class HeaderStampingHandler : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            request.Headers.TryAddWithoutValidation("X-Stamped", "yes");
            return base.SendAsync(request, ct);
        }
    }
}
