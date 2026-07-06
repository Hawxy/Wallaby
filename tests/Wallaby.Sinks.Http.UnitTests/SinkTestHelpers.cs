using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Http.UnitTests;

/// <summary>Message handler that records every request (with its body) and returns configurable responses.</summary>
internal sealed class CapturingHandler : HttpMessageHandler
{
    public sealed record Captured(HttpRequestMessage Request, byte[] Body, string? Signature, string? ContentEncoding);

    public List<Captured> Requests { get; } = [];

    /// <summary>Response per zero-based request index; defaults to 200 OK.</summary>
    public Func<int, HttpResponseMessage> Respond { get; set; } = _ => new HttpResponseMessage(HttpStatusCode.OK);

    /// <summary>When set, thrown instead of responding.</summary>
    public Exception? Throw { get; set; }

    /// <summary>Extra per-request work (e.g. delays for timeout tests); receives the request cancellation token.</summary>
    public Func<CancellationToken, Task>? OnRequest { get; set; }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var body = await request.Content!.ReadAsByteArrayAsync(ct);
        var signature = request.Headers.TryGetValues(HttpSink.SignatureHeader, out var values)
            ? values.Single()
            : null;
        Requests.Add(new Captured(request, body, signature, request.Content.Headers.ContentEncoding.SingleOrDefault()));

        if (OnRequest is not null)
        {
            await OnRequest(ct);
        }
        if (Throw is not null)
        {
            throw Throw;
        }
        return Respond(Requests.Count - 1);
    }
}

internal static class SinkTestHelpers
{
    public const string SinkName = "webhook";

    /// <summary>A sink wired to a real IHttpClientFactory whose named client sends into <paramref name="handler"/>.</summary>
    public static HttpSink CreateSink(CapturingHandler handler, Action<HttpSinkOptions>? configure = null)
    {
        var options = new HttpSinkOptions { Endpoint = "https://receiver.example/hooks" };
        configure?.Invoke(options);

        var services = new ServiceCollection();
        services.AddHttpClient(options.HttpClientName ?? HttpSink.ClientNameFor(SinkName))
            .ConfigurePrimaryHttpMessageHandler(() => handler);
        var factory = services.BuildServiceProvider().GetRequiredService<IHttpClientFactory>();

        return new HttpSink(SinkName, options, factory);
    }

    public static ChangeMetadata Meta(int commitIdx = 0, bool backfill = false, DateTimeOffset? timestamp = null, ulong lsn = 12345)
        => new("public", "products", timestamp, lsn, commitIdx, backfill);

    public static SinkRecord Upsert(string id, IReadOnlyDictionary<string, object?> document,
        string? destination = "products", ChangeMetadata? metadata = null)
        => new(destination, id, document, false, metadata ?? Meta());

    public static SinkRecord Delete(string id, ChangeMetadata? metadata = null)
        => new("products", id, null, true, metadata ?? Meta());

    public static SinkBatch Batch(params SinkRecord[] records) => new(SinkName, records);
}
