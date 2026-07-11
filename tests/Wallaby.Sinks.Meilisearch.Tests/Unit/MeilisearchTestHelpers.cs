using System.Net;
using System.Text;
using System.Text.Json;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Meilisearch.Tests.Unit;

/// <summary>
/// Message handler standing in for a Meilisearch server, plugged in as the sink's transport (the sink
/// wraps it in the SDK's MeilisearchMessageHandler, which converts error responses into
/// <c>MeilisearchApiError</c>). The default <see cref="Respond"/> is a happy-path simulator: every write
/// enqueues a task and every task poll reports it succeeded.
/// </summary>
internal sealed class StubHandler : HttpMessageHandler
{
    private readonly MeiliSimulator _simulator = new();

    public List<HttpRequestMessage> Requests { get; } = [];

    /// <summary>Operations the happy-path simulator served, e.g. <c>add:3</c>, <c>delete:2</c>.</summary>
    public List<string> Operations => _simulator.Operations;

    public Func<HttpRequestMessage, string?, HttpResponseMessage>? Respond { get; set; }

    /// <summary>When set, thrown instead of responding (transport failure).</summary>
    public Exception? Throw { get; set; }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        Requests.Add(request);
        var body = request.Content is null ? null : await request.Content.ReadAsStringAsync(ct);

        if (Throw is not null)
        {
            throw Throw;
        }
        return Respond is not null ? Respond(request, body) : _simulator.Respond(request, body);
    }
}

/// <summary>Happy-path Meilisearch: writes enqueue incrementing task uids; task polls succeed.</summary>
internal sealed class MeiliSimulator
{
    private int _uid;

    public List<string> Operations { get; } = [];

    public HttpResponseMessage Respond(HttpRequestMessage request, string? body)
    {
        var path = request.RequestUri!.AbsolutePath;

        if (request.Method == HttpMethod.Get && path.StartsWith("/tasks/", StringComparison.Ordinal))
        {
            var uid = int.Parse(path["/tasks/".Length..]);
            return MeilisearchTestHelpers.Json(HttpStatusCode.OK, MeilisearchTestHelpers.TaskResultJson(uid, "succeeded"));
        }

        if (request.Method == HttpMethod.Get && path.StartsWith("/indexes/", StringComparison.Ordinal))
        {
            var name = path["/indexes/".Length..];
            return MeilisearchTestHelpers.Json(
                HttpStatusCode.OK,
                $$"""{"uid":"{{name}}","primaryKey":"id","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"}""");
        }

        if (path.EndsWith("/documents/delete-batch", StringComparison.Ordinal))
        {
            Operations.Add($"delete:{JsonDocument.Parse(body!).RootElement.GetArrayLength()}");
        }
        else if (path.EndsWith("/documents", StringComparison.Ordinal))
        {
            Operations.Add($"add:{JsonDocument.Parse(body!).RootElement.GetArrayLength()}");
        }

        // Index creation, settings updates, and document writes all enqueue a task.
        return MeilisearchTestHelpers.Json(HttpStatusCode.Accepted, MeilisearchTestHelpers.TaskInfoJson(++_uid));
    }
}

internal static class MeilisearchTestHelpers
{
    public static HttpResponseMessage Json(HttpStatusCode status, string json)
        => new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    public static string TaskInfoJson(int uid)
        => $$"""{"taskUid":{{uid}},"indexUid":"idx","status":"enqueued","type":"documentAdditionOrUpdate","enqueuedAt":"2026-01-01T00:00:00Z"}""";

    public static string TaskResultJson(int uid, string status, string? errorCode = null)
    {
        var error = errorCode is null
            ? string.Empty
            : $$""","error":{"message":"boom","code":"{{errorCode}}","type":"invalid_request","link":"https://docs.meilisearch.com/errors"}""";
        return $$"""{"uid":{{uid}},"indexUid":"idx","status":"{{status}}","type":"documentAdditionOrUpdate","enqueuedAt":"2026-01-01T00:00:00Z"{{error}}}""";
    }

    public static string ApiErrorJson(string code)
        => $$"""{"message":"boom","code":"{{code}}","type":"invalid_request","link":"https://docs.meilisearch.com/errors"}""";

    /// <summary>A sink whose transport is <paramref name="stub"/> directly (no factory required).</summary>
    public static MeilisearchSink Sink(StubHandler stub, Action<MeilisearchSinkOptions>? configure = null)
    {
        var options = new MeilisearchSinkOptions { Host = "http://meili.local" };
        configure?.Invoke(options);
        return new MeilisearchSink("meili", options, () => stub);
    }

    public static ChangeMetadata Meta(ChangeAction action = ChangeAction.Insert)
        => new("public", "products", action, DateTimeOffset.UtcNow, 1, 0, false);

    public static SinkRecord Upsert(string id, string? destination = "products")
        => new(destination, id, new WallabyDocument { ["name"] = "n" + id }, IsDeletion: false, Meta());

    public static SinkRecord Delete(string id, string? destination = "products")
        => new(destination, id, null, IsDeletion: true, Meta(ChangeAction.Delete));

    public static SinkBatch Batch(params SinkRecord[] records) => new("meili", records);
}
