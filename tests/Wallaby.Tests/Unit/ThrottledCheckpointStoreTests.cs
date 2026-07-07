using Wallaby.Abstractions;
using Wallaby.Internal.State;

namespace Wallaby.Tests.Unit;

public class ThrottledCheckpointStoreTests
{
    private sealed class CountingStore : ICheckpointStore
    {
        public int Saves;
        public Checkpoint? Stored;

        public Task<Checkpoint?> GetAsync(string slotName, CancellationToken ct) => Task.FromResult(Stored);

        public Task SaveAsync(string slotName, Checkpoint checkpoint, CancellationToken ct)
        {
            Saves++;
            Stored = checkpoint;
            return Task.CompletedTask;
        }
    }

    // 1 timestamp tick = 1 ms.
    private sealed class ManualClock : TimeProvider
    {
        public long Ticks;
        public override long GetTimestamp() => Ticks;
        public override long TimestampFrequency => 1000;
    }

    [Test]
    public async Task First_save_always_writes()
    {
        var inner = new CountingStore();
        var store = new ThrottledCheckpointStore(inner, TimeSpan.FromSeconds(5), new ManualClock());

        await store.SaveAsync("slot", new Checkpoint(1, DateTimeOffset.UtcNow), CancellationToken.None);

        inner.Saves.ShouldBe(1);
    }

    [Test]
    public async Task Saves_within_the_interval_are_skipped()
    {
        var inner = new CountingStore();
        var clock = new ManualClock();
        var store = new ThrottledCheckpointStore(inner, TimeSpan.FromSeconds(5), clock);

        await store.SaveAsync("slot", new Checkpoint(1, DateTimeOffset.UtcNow), CancellationToken.None);
        clock.Ticks += 2_000;
        await store.SaveAsync("slot", new Checkpoint(2, DateTimeOffset.UtcNow), CancellationToken.None);

        inner.Saves.ShouldBe(1);
        inner.Stored!.ConfirmedLsn.ShouldBe(1UL);

        clock.Ticks += 4_000;
        await store.SaveAsync("slot", new Checkpoint(3, DateTimeOffset.UtcNow), CancellationToken.None);

        inner.Saves.ShouldBe(2);
        inner.Stored!.ConfirmedLsn.ShouldBe(3UL);
    }

    [Test]
    public async Task Get_passes_through()
    {
        var inner = new CountingStore { Stored = new Checkpoint(7, DateTimeOffset.UtcNow) };
        var store = new ThrottledCheckpointStore(inner, TimeSpan.FromSeconds(5), new ManualClock());

        var checkpoint = await store.GetAsync("slot", CancellationToken.None);

        checkpoint!.ConfirmedLsn.ShouldBe(7UL);
    }

    [Test]
    public async Task Failed_save_does_not_advance_the_window()
    {
        var clock = new ManualClock();
        var failing = new FailOnceStore();
        var store = new ThrottledCheckpointStore(failing, TimeSpan.FromSeconds(5), clock);

        await Should.ThrowAsync<InvalidOperationException>(
            async () => await store.SaveAsync("slot", new Checkpoint(1, DateTimeOffset.UtcNow), CancellationToken.None));

        clock.Ticks += 1;
        await store.SaveAsync("slot", new Checkpoint(2, DateTimeOffset.UtcNow), CancellationToken.None);

        failing.Saves.ShouldBe(1);
    }

    private sealed class FailOnceStore : ICheckpointStore
    {
        private bool _failed;
        public int Saves;

        public Task<Checkpoint?> GetAsync(string slotName, CancellationToken ct) => Task.FromResult<Checkpoint?>(null);

        public Task SaveAsync(string slotName, Checkpoint checkpoint, CancellationToken ct)
        {
            if (!_failed)
            {
                _failed = true;
                throw new InvalidOperationException("boom");
            }
            Saves++;
            return Task.CompletedTask;
        }
    }
}
