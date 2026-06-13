using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.TestModel;

namespace Wallaby.UnitTests;

/// <summary>
/// Routing semantics for a single batch (one transaction's changes, or one dispatched slice). The key
/// invariant: a key that appears multiple times in the batch must resolve to its <em>last</em> action in
/// commit order — exactly one routed record per key, never both an upsert and a deletion.
/// </summary>
public class MappingChangeRouterTests
{
    /// <summary>Emits one document per change (so every non-delete becomes an upsert).</summary>
    private sealed class PassthroughTransform : ITransformInvoker
    {
        public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
            DbContext db, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
        {
            var documents = new Dictionary<DocumentKey, WallabyDocument?>();
            foreach (var change in changes)
            {
                documents[change.Key] = new WallabyDocument { ["id"] = change.Key.ToString() };
            }
            return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
        }
    }

    /// <summary>A transform that always throws — to exercise the dead-letter policy.</summary>
    private sealed class ThrowingTransform : ITransformInvoker
    {
        public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
            DbContext db, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
            => throw new InvalidOperationException("boom");
    }

    private static MappingChangeRouter Router(ITransformInvoker? transform = null, bool skipFailedBatches = false)
    {
        var mapping = new EntityMapping
        {
            EntityClrType = typeof(Product),
            SinkName = "sink",
            Destination = "products",
            Transform = transform ?? new PassthroughTransform(),
        };
        var contextProvider = new DefaultEnrichmentContextProvider(
            () => new AppDbContext(TestModelFactory.CreateOptions("Host=localhost;Username=u;Password=p;Database=d")));
        return new MappingChangeRouter(
            new Dictionary<Type, EntityMapping> { [typeof(Product)] = mapping }, contextProvider,
            skipFailedBatches: skipFailedBatches);
    }

    private static ChangeEvent Change(ChangeAction action, int id)
    {
        var meta = new ChangeMetadata("public", "products", DateTimeOffset.UtcNow, 1, 0, IsBackfill: false);
        return new ChangeEvent(
            action, meta, new Product { Id = id, Name = "x" },
            new Dictionary<string, object?>(), Changes: null, new object[] { id })
        {
            EntityClrType = typeof(Product),
        };
    }

    [Test]
    public async Task Insert_then_delete_of_one_key_routes_a_single_deletion()
    {
        var routed = await Router().RouteAsync(
            [Change(ChangeAction.Insert, 1), Change(ChangeAction.Delete, 1)], CancellationToken.None);

        routed.Count.ShouldBe(1);
        routed[0].Record.IsDeletion.ShouldBeTrue();
    }

    [Test]
    public async Task Update_then_delete_of_one_key_routes_a_single_deletion()
    {
        var routed = await Router().RouteAsync(
            [Change(ChangeAction.Update, 1), Change(ChangeAction.Delete, 1)], CancellationToken.None);

        routed.Count.ShouldBe(1);
        routed[0].Record.IsDeletion.ShouldBeTrue();
    }

    [Test]
    public async Task Delete_then_reinsert_of_one_key_routes_a_single_upsert()
    {
        var routed = await Router().RouteAsync(
            [Change(ChangeAction.Delete, 1), Change(ChangeAction.Insert, 1)], CancellationToken.None);

        routed.Count.ShouldBe(1);
        routed[0].Record.IsDeletion.ShouldBeFalse();
    }

    [Test]
    public async Task Distinct_keys_are_routed_independently()
    {
        var routed = await Router().RouteAsync(
            [Change(ChangeAction.Insert, 1), Change(ChangeAction.Delete, 2)], CancellationToken.None);

        routed.Count.ShouldBe(2);
        routed.Count(r => r.Record.IsDeletion).ShouldBe(1);
        routed.Count(r => !r.Record.IsDeletion).ShouldBe(1);
    }

    [Test]
    public async Task Transform_failure_halts_by_default()
    {
        var router = Router(new ThrowingTransform(), skipFailedBatches: false);

        await Should.ThrowAsync<InvalidOperationException>(
            async () => await router.RouteAsync([Change(ChangeAction.Insert, 1)], CancellationToken.None));
    }

    [Test]
    public async Task Transform_failure_is_dead_lettered_when_policy_is_skip()
    {
        var router = Router(new ThrowingTransform(), skipFailedBatches: true);

        var routed = await router.RouteAsync([Change(ChangeAction.Insert, 1)], CancellationToken.None);

        // The poison batch is dropped rather than throwing, so the pipeline can keep streaming.
        routed.Count.ShouldBe(0);
    }
}
