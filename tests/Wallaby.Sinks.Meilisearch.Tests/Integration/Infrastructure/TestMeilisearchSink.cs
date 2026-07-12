namespace Wallaby.Sinks.Meilisearch.Tests.Integration.Infrastructure;

/// <summary>Builds sinks through the internal transport ctor, bypassing the IHttpMessageHandlerFactory requirement.</summary>
internal static class TestMeilisearchSink
{
    public static MeilisearchSink Create(string name, MeilisearchSinkOptions options)
    {
        // One handler per sink so its HTTP connections pool across deliveries.
        var handler = new HttpClientHandler();
        return new MeilisearchSink(name, options, () => handler);
    }
}
