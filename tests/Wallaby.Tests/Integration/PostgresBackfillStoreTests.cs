using System.Diagnostics;
using Wallaby.Abstractions;
using Wallaby.Internal.State;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

/// <summary>
/// Manual backfill requests win over a running backfill's progress writes (so a mid-run request is never
/// clobbered) and wake the leader's scheduler via LISTEN/NOTIFY.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class PostgresBackfillStoreTests(PostgresFixture pg)
{
    private static string UniqueTable(string hint) => $"public.{hint}_{Guid.NewGuid():N}";

    private static BackfillState State(string table, BackfillStatus status, string? cursorJson = null, long rows = 0)
        => new(table, status, "v1", cursorJson, rows, DateTimeOffset.UtcNow);

    [Test]
    public async Task Progress_saves_do_not_overwrite_a_concurrent_request()
    {
        await EnsureSchemaAsync();
        var store = new PostgresBackfillStore(pg.DataSource);
        var table = UniqueTable("orders");

        await store.SaveAsync(State(table, BackfillStatus.InProgress), CancellationToken.None);
        await store.RequestAsync(table, "v1", purge: false, CancellationToken.None);

        // The running backfill keeps writing progress (including its final Completed) — all no-ops now.
        await store.SaveProgressAsync(
            State(table, BackfillStatus.InProgress, """{"v":1}""", rows: 42), CancellationToken.None);
        await store.SaveProgressAsync(State(table, BackfillStatus.Completed, rows: 99), CancellationToken.None);

        var state = await store.GetAsync(table, CancellationToken.None);
        state.ShouldNotBeNull();
        state.Status.ShouldBe(BackfillStatus.Requested);
        state.CursorJson.ShouldBeNull();
        state.RowsCopied.ShouldBe(0);
    }

    [Test]
    public async Task Progress_saves_apply_while_the_run_is_unchallenged()
    {
        await EnsureSchemaAsync();
        var store = new PostgresBackfillStore(pg.DataSource);
        var table = UniqueTable("orders");

        await store.SaveAsync(State(table, BackfillStatus.InProgress), CancellationToken.None);
        await store.SaveProgressAsync(
            State(table, BackfillStatus.InProgress, """{"v":1}""", rows: 42), CancellationToken.None);

        var state = await store.GetAsync(table, CancellationToken.None);
        state.ShouldNotBeNull();
        state.Status.ShouldBe(BackfillStatus.InProgress);
        state.CursorJson.ShouldNotBeNull();
        state.RowsCopied.ShouldBe(42);

        await store.SaveProgressAsync(State(table, BackfillStatus.Completed, rows: 99), CancellationToken.None);
        (await store.GetAsync(table, CancellationToken.None))!.Status.ShouldBe(BackfillStatus.Completed);
    }

    [Test]
    public async Task Requesting_a_backfill_wakes_a_subscriber()
    {
        await EnsureSchemaAsync();
        var store = new PostgresBackfillStore(pg.DataSource);

        await using var subscription = store.Subscribe();
        // Prime the subscription so it is LISTENing before the request — otherwise the NOTIFY is missed.
        await subscription.WaitAsync(TimeSpan.FromMilliseconds(100), CancellationToken.None);

        await store.RequestAsync(UniqueTable("orders"), null, purge: false, CancellationToken.None);

        var stopwatch = Stopwatch.StartNew();
        await subscription.WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        stopwatch.Stop();

        // Woken by the NOTIFY, not the 30s fallback poll.
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task Purge_mark_is_sticky_until_a_fresh_run_clears_it()
    {
        await EnsureSchemaAsync();
        var store = new PostgresBackfillStore(pg.DataSource);
        var table = UniqueTable("orders");

        await store.RequestAsync(table, "v1", purge: true, CancellationToken.None);
        (await store.GetAsync(table, CancellationToken.None))!.Purge.ShouldBeTrue();

        // A racing plain request must not clear the pending purge.
        await store.RequestAsync(table, "v1", purge: false, CancellationToken.None);
        (await store.GetAsync(table, CancellationToken.None))!.Purge.ShouldBeTrue();

        // The scheduler's fresh-run transition clears it (Purge defaults false).
        await store.SaveAsync(State(table, BackfillStatus.InProgress), CancellationToken.None);
        (await store.GetAsync(table, CancellationToken.None))!.Purge.ShouldBeFalse();
    }

    [Test]
    public async Task List_requested_filters_to_the_given_tables()
    {
        await EnsureSchemaAsync();
        var store = new PostgresBackfillStore(pg.DataSource);
        var requested = UniqueTable("a");
        var inProgress = UniqueTable("b");

        await store.RequestAsync(requested, null, purge: false, CancellationToken.None);
        await store.SaveAsync(State(inProgress, BackfillStatus.InProgress), CancellationToken.None);

        var listed = await store.ListRequestedAsync(
            [requested, inProgress, UniqueTable("absent")], CancellationToken.None);

        listed.ShouldBe([requested]);
    }

    private async Task EnsureSchemaAsync()
    {
        await using var conn = await pg.DataSource.OpenConnectionAsync();
        await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
    }
}
