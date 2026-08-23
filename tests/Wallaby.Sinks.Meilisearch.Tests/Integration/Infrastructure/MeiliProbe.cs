using System.Text.Json.Nodes;
using Meilisearch;

namespace Wallaby.Sinks.Meilisearch.Tests.Integration.Infrastructure;

/// <summary>Small helper for asserting on / managing a Meilisearch index in tests.</summary>
public sealed class MeiliProbe(string host, string apiKey)
{
    private readonly MeilisearchClient _client = new(host, apiKey);

    public MeiliProbe(MeilisearchFixture fixture) : this(fixture.Host, fixture.ApiKey) { }

    /// <summary>The document for <paramref name="id"/>, or null if the document/index is absent.</summary>
    public async Task<JsonObject?> GetAsync(string index, string id)
    {
        try
        {
            return await _client.Index(index).GetDocumentAsync<JsonObject>(id);
        }
        catch (MeilisearchApiError)
        {
            return null;
        }
    }

    /// <summary>The <c>name</c> field of the document for <paramref name="id"/>, or null if absent.</summary>
    public async Task<string?> NameAsync(string index, int id)
        => (await GetAsync(index, id.ToString()))?["name"]?.GetValue<string>();

    /// <summary>True when the index exists.</summary>
    public async Task<bool> IndexExistsAsync(string index)
    {
        try
        {
            await _client.GetIndexAsync(index);
            return true;
        }
        catch (MeilisearchApiError)
        {
            return false;
        }
    }

    /// <summary>The configured primary key of the index, or null if unset/absent.</summary>
    public async Task<string?> PrimaryKeyAsync(string index)
    {
        try
        {
            return await _client.Index(index).FetchPrimaryKey();
        }
        catch (MeilisearchApiError)
        {
            return null;
        }
    }

    /// <summary>The index's current settings.</summary>
    public Task<Settings> SettingsAsync(string index) => _client.Index(index).GetSettingsAsync();

    /// <summary>The index's configured embedders (empty when none).</summary>
    public Task<Dictionary<string, Embedder>> EmbeddersAsync(string index) => _client.Index(index).GetEmbeddersAsync();

    /// <summary>Delete the index and wait for the task to finish (used to prove a re-backfill repopulates it).</summary>
    public async Task DropAsync(string index)
    {
        var info = await _client.DeleteIndexAsync(index);
        await _client.WaitForTaskAsync(info.TaskUid, 30_000, 50, CancellationToken.None);
    }
}
