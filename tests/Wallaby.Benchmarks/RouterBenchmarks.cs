using BenchmarkDotNet.Attributes;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Providers;

namespace Wallaby.Benchmarks;

/// <summary>
/// Routing cost per transaction shape: a single change (the steady-state hot path) and a
/// 100-change burst, through a no-op transform.
/// </summary>
[MemoryDiagnoser]
public class RouterBenchmarks
{
    private sealed class Doc;

    private MappingChangeRouter _router = null!;
    private IReadOnlyList<ChangeEvent> _single = null!;
    private IReadOnlyList<ChangeEvent> _hundred = null!;

    [GlobalSetup]
    public void Setup()
    {
        var mapping = new EntityMapping
        {
            EntityClrType = typeof(Doc),
            SinkName = "sink",
            Destination = "dest",
            Transform = new UpsertAllTransform(),
            Sessions = new NoOpSessionProvider(),
        };
        _router = new MappingChangeRouter([mapping]);
        _single = [Change(1)];
        _hundred = [.. Enumerable.Range(1, 100).Select(Change)];
    }

    [Benchmark]
    public async Task<int> RouteSingleChange()
        => (await _router.RouteAsync(_single, CancellationToken.None)).Count;

    [Benchmark]
    public async Task<int> RouteHundredChanges()
        => (await _router.RouteAsync(_hundred, CancellationToken.None)).Count;

    private static ChangeEvent Change(int id)
    {
        var meta = new ChangeMetadata("public", "t", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, IsBackfill: false);
        return new ChangeEvent(ChangeAction.Insert, meta, Entity: id, new Dictionary<string, object?>(), Changes: null, [id])
        {
            EntityClrType = typeof(Doc),
        };
    }

    private sealed class UpsertAllTransform : IWallabyTransformInvoker
    {
        public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
            object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
        {
            var documents = new Dictionary<DocumentKey, WallabyDocument?>(changes.Count);
            foreach (var change in changes)
            {
                documents[change.Key] = new WallabyDocument { ["id"] = change.Key.ToString() };
            }
            return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
        }
    }

    private sealed class NoOpSessionProvider : IEnrichmentSessionProvider
    {
        public bool IsScoped => false;
        public IEnrichmentSession Lease(object? scopeKey) => new NoOpSession();
    }

    private sealed class NoOpSession : IEnrichmentSession
    {
        public object Session => this;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
