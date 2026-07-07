using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Providers;

namespace Wallaby.Tests.Unit;

/// <summary>
/// Routing semantics for a single batch (one transaction's changes, or one dispatched slice). The key
/// invariant: a key that appears multiple times in the batch must resolve to its <em>last</em> action in
/// commit order — exactly one routed record per key, never both an upsert and a deletion.
/// </summary>
public class MappingChangeRouterTests
{
    private sealed class Doc;

    /// <summary>A transform that always throws — to exercise the halt-on-transform-failure behavior.</summary>
    private sealed class ThrowingTransform : IWallabyTransformInvoker
    {
        public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
            object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
            => throw new InvalidOperationException("boom");
    }

    private static MappingChangeRouter Router(IWallabyTransformInvoker? transform = null)
        => new([TestChanges.Mapping(typeof(Doc), transform ?? new RecordingTransform(), new FakeSessionProvider())]);

    private static ChangeEvent Change(ChangeAction action, int id) => TestChanges.Change(typeof(Doc), id, action);

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
    public async Task Transform_failure_always_halts()
    {
        var router = Router(new ThrowingTransform());

        await Should.ThrowAsync<InvalidOperationException>(
            async () => await router.RouteAsync([Change(ChangeAction.Insert, 1)], CancellationToken.None));
    }
}
