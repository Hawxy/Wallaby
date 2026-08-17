using System.Net;
using System.Text.Json.Nodes;

namespace Wallaby.Sinks.Elasticsearch.Tests.Integration.Infrastructure;

/// <summary>
/// Small helper for asserting on Elasticsearch documents in tests. Deliberately client-free (raw REST via
/// <see cref="HttpClient"/>) so it double-checks the sink's writes independently of the client library.
/// </summary>
public sealed class ElasticsearchProbe(string endpoint) : IDisposable
{
    private readonly HttpClient _http = new() { BaseAddress = new Uri(endpoint) };

    public ElasticsearchProbe(ElasticsearchFixture fixture) : this(fixture.Endpoint) { }

    /// <summary>The document source for <paramref name="id"/>, or null if the document/index is absent.</summary>
    public async Task<JsonObject?> GetAsync(string index, string id)
    {
        using var response = await _http.GetAsync($"{index}/_doc/{id}");
        // 404: document/index absent. 503: the auto-created index's primary shard is still allocating —
        // "not there yet" either way, so polling callers retry.
        if (response.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.ServiceUnavailable)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        var body = JsonNode.Parse(await response.Content.ReadAsStringAsync())!.AsObject();
        return body["found"]?.GetValue<bool>() == true ? body["_source"]?.AsObject() : null;
    }

    /// <summary>The <c>name</c> field of the document for <paramref name="id"/>, or null if absent.</summary>
    public async Task<string?> NameAsync(string index, int id)
        => (await GetAsync(index, id.ToString()))?["name"]?.GetValue<string>();

    public void Dispose() => _http.Dispose();
}
