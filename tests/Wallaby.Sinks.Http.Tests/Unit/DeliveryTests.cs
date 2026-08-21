using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using StandardWebhooks;
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
    public async Task A_redirect_response_is_permanent_and_names_the_location()
    {
        // The sink never follows redirects: doing so would rewrite POST→GET and drop the body.
        var handler = new CapturingHandler
        {
            Respond = _ => new HttpResponseMessage(HttpStatusCode.Found)
            {
                Headers = { Location = new Uri("https://receiver.example/hooks-v2") },
            },
        };
        var sink = CreateSink(handler);

        var result = await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error.ShouldNotBeNull().ShouldContain("https://receiver.example/hooks-v2");
        result.Error.ShouldContain("redirect");
    }

    [Test]
    public async Task A_followed_redirect_on_a_custom_client_is_permanent_not_success()
    {
        // A user-supplied client may still follow redirects; the terminal 2xx then comes from a URI the
        // body never reached. Simulated by answering 200 from a different final request URI.
        var handler = new CapturingHandler
        {
            Respond = _ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                RequestMessage = new HttpRequestMessage(HttpMethod.Get, "https://receiver.example/hooks-v2"),
            },
        };
        var sink = CreateSink(handler);

        var result = await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error.ShouldNotBeNull().ShouldContain("https://receiver.example/hooks-v2");
    }

    [Test]
    public async Task A_uri_rewriting_handler_does_not_trip_the_redirect_defense()
    {
        // Service discovery / proxy handlers mutate the request URI before dispatch; the response then
        // legitimately comes from the rewritten URI and must stay a success.
        var handler = new CapturingHandler();
        var services = new ServiceCollection();
        services.AddTransient<UriRewritingHandler>();
        services.AddHttpClient(HttpSink.ClientNameFor(SinkName))
            .AddHttpMessageHandler<UriRewritingHandler>()
            .ConfigurePrimaryHttpMessageHandler(() => handler);
        var factory = services.BuildServiceProvider().GetRequiredService<IHttpClientFactory>();
        var sink = new HttpSink(SinkName, new HttpSinkOptions { Endpoint = "https://receiver.example/hooks" }, factory);

        var result = await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        handler.Requests.ShouldHaveSingleItem().Request.RequestUri!
            .Host.ShouldBe("resolved.internal");
    }

    private sealed class UriRewritingHandler : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            request.RequestUri = new UriBuilder(request.RequestUri!) { Host = "resolved.internal" }.Uri;
            return base.SendAsync(request, ct);
        }
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

    private const string Secret = "whsec_dGVzdC1zaWduaW5nLWtleS0wMTIzNDU2Nzg5YWJjZGVm";
    private const string PreviousSecret = "whsec_cHJldmlvdXMtc2lnbmluZy1rZXktMDAwMDAwMDAwMDAw";

    // The independent reference: the StandardWebhooks package's signer, fed the captured id, timestamp,
    // and body, must reproduce the header exactly.
    private static string ReferenceSignature(string secret, CapturingHandler.Captured captured)
        => new StandardWebhook(secret).Sign(
            captured.Id!,
            DateTimeOffset.FromUnixTimeSeconds(long.Parse(captured.Timestamp!)),
            Encoding.UTF8.GetString(captured.Body));

    [Test]
    public async Task Requests_carry_a_standard_webhooks_signature_when_a_secret_is_configured()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler, o => o.SigningSecret = Secret);

        (await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        var captured = handler.Requests.ShouldHaveSingleItem();
        captured.Id.ShouldNotBeNull();
        captured.Id!.ShouldStartWith("msg_");
        captured.Timestamp.ShouldNotBeNull();
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long.Parse(captured.Timestamp!).ShouldBeInRange(now - 60, now + 5);

        captured.Signature.ShouldBe(ReferenceSignature(Secret, captured));
    }

    [Test]
    public async Task A_retried_delivery_carries_the_same_message_id()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler, o => o.SigningSecret = Secret);

        await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None);
        await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None);
        await sink.DeliverAsync(Batch(Upserts("2")), CancellationToken.None);

        handler.Requests.Count.ShouldBe(3);
        handler.Requests[1].Id.ShouldBe(handler.Requests[0].Id);
        handler.Requests[2].Id.ShouldNotBe(handler.Requests[0].Id);
    }

    [Test]
    public async Task A_previous_secret_adds_a_second_verifiable_signature()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler, o =>
        {
            o.SigningSecret = Secret;
            o.PreviousSigningSecret = PreviousSecret;
        });

        (await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        var captured = handler.Requests.ShouldHaveSingleItem();
        var signatures = captured.Signature!.Split(' ');
        signatures.Length.ShouldBe(2);
        signatures[0].ShouldBe(ReferenceSignature(Secret, captured));
        signatures[1].ShouldBe(ReferenceSignature(PreviousSecret, captured));
    }

    [Test]
    public void A_non_base64_secret_fails_construction_with_guidance()
    {
        var ex = Should.Throw<WallabyConfigurationException>(
            () => CreateSink(new CapturingHandler(), o => o.SigningSecret = "not base64!"));

        ex.Message.ShouldContain("whsec_");
    }

    [Test]
    [Arguments("")]
    [Arguments("whsec_")]
    [Arguments("c2hvcnQ=")] // 5 key bytes
    public void A_secret_below_the_minimum_key_length_fails_construction(string secret)
    {
        var ex = Should.Throw<WallabyConfigurationException>(
            () => CreateSink(new CapturingHandler(), o => o.SigningSecret = secret));

        ex.Message.ShouldContain("at least 16");
    }

    [Test]
    public void A_previous_secret_below_the_minimum_key_length_fails_construction()
    {
        Should.Throw<WallabyConfigurationException>(
            () => CreateSink(new CapturingHandler(), o =>
            {
                o.SigningSecret = Secret;
                o.PreviousSigningSecret = "whsec_";
            }));
    }

    [Test]
    public void A_previous_secret_without_an_active_secret_fails_construction()
    {
        Should.Throw<WallabyConfigurationException>(
            () => CreateSink(new CapturingHandler(), o => o.PreviousSigningSecret = PreviousSecret));
    }

    [Test]
    public async Task Requests_are_unsigned_without_a_secret()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler);

        (await sink.DeliverAsync(Batch(Upserts("1")), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        var captured = handler.Requests.ShouldHaveSingleItem();
        captured.Signature.ShouldBeNull();
        captured.Timestamp.ShouldBeNull();
        captured.Id.ShouldBeNull();
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
