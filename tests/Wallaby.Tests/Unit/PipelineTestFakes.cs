using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Providers;

namespace Wallaby.Tests.Unit;

/// <summary>Session that reports its lease/disposal back to the owning provider.</summary>
internal sealed class FakeSession(FakeSessionProvider owner) : IEnrichmentSession
{
    public object Session => owner;
    public ValueTask DisposeAsync()
    {
        owner.Disposals++;
        return ValueTask.CompletedTask;
    }
}

/// <summary>Counts leases and disposals so tests can assert session-per-batch semantics.</summary>
internal sealed class FakeSessionProvider : IEnrichmentSessionProvider
{
    public int Leases { get; private set; }
    public int Disposals { get; set; }
    public bool IsScoped => false;

    public IEnrichmentSession Lease(object? scopeKey)
    {
        Leases++;
        return new FakeSession(this);
    }
}

/// <summary>Transform that records the session each invocation received and upserts every change.</summary>
internal sealed class RecordingTransform : IWallabyTransformInvoker
{
    public List<object> Sessions { get; } = [];

    public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
        object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
    {
        Sessions.Add(session);
        var documents = new Dictionary<DocumentKey, WallabyDocument?>();
        foreach (var change in changes)
        {
            documents[change.Key] = new WallabyDocument { ["id"] = change.Key.ToString() };
        }
        return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
    }
}

/// <summary>Factories for the hand-built mappings/changes the router and multi-provider tests share.</summary>
internal static class TestChanges
{
    public static EntityMapping Mapping(Type type, IWallabyTransformInvoker transform, IEnrichmentSessionProvider sessions)
        => new()
        {
            EntityClrType = type, SinkName = "sink", Destination = "dest", Transform = transform, Sessions = sessions,
        };

    public static ChangeEvent Change(Type type, int id, ChangeAction action = ChangeAction.Insert)
    {
        var meta = new ChangeMetadata("public", "t", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, IsBackfill: false);
        return new ChangeEvent(
            action, meta, Entity: id, new Dictionary<string, object?>(), Changes: null, [id])
        {
            EntityClrType = type,
        };
    }
}
