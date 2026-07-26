using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Tests.Unit;

// A flushed or overflowed binding never reads an inline page, so an unopenable data source is enough
// for every test here; the inline-page paths are covered by the EF integration suite.
public class DependentChangeResolverTests
{
    [Test]
    public async Task A_wide_lookup_set_is_offloaded_in_bounded_chunk_jobs()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var resolver = new DependentChangeResolver(
            dataSource, BuildModel(), instrumentation: null, maxKeysPerTransaction: 1_000_000, chunkSize: 4);
        var chunks = new List<ScopedFanoutSpec>();

        var results = await resolver.ResolveFirstPagesAsync(
            DistinctLookupChanges(10), pageSize: 100,
            (spec, _) => { chunks.Add(spec); return Task.CompletedTask; }, CancellationToken.None);

        // 10 distinct keys with chunkSize 4: two full chunks mid-consumption plus the final partial one.
        chunks.Select(c => c.LookupValues.Count).ShouldBe([4, 4, 2]);
        chunks.SelectMany(c => c.LookupValues).Select(t => t[0]).ShouldBe(
            Enumerable.Range(0, 10).Cast<object?>(), ignoreOrder: true);
        chunks.ShouldAllBe(c => c.PrimaryTable.QualifiedName == "public.products");
        results.ShouldBeEmpty(); // the queue owns the whole scope; no inline page
    }

    [Test]
    public async Task Chunks_are_deterministic_across_a_redelivery()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");

        // Identical chunk key sets produce identical lookup hashes, which is what lets a redelivered
        // transaction's re-enqueued chunks coalesce instead of duplicating jobs.
        var runs = new List<List<string>>();
        for (var run = 0; run < 2; run++)
        {
            var resolver = new DependentChangeResolver(
                dataSource, BuildModel(), instrumentation: null, maxKeysPerTransaction: 1_000_000, chunkSize: 3);
            var hashes = new List<string>();
            await resolver.ResolveFirstPagesAsync(
                DistinctLookupChanges(8), pageSize: 100,
                (spec, _) =>
                {
                    hashes.Add(PostgresFanoutQueueStore.Hash(
                        spec.PrimaryTable.QualifiedName,
                        [.. spec.LookupColumns],
                        PostgresFanoutQueueStore.CanonicalValuesJson(spec.LookupValues)));
                    return Task.CompletedTask;
                },
                CancellationToken.None);
            runs.Add(hashes);
        }

        runs[0].Count.ShouldBe(3);
        runs[1].ShouldBe(runs[0]);
    }

    [Test]
    public async Task Repeated_keys_dedupe_across_chunks()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var resolver = new DependentChangeResolver(
            dataSource, BuildModel(), instrumentation: null, maxKeysPerTransaction: 1_000_000, chunkSize: 4);
        var chunks = new List<ScopedFanoutSpec>();

        // 21 changes cycling 6 distinct keys; the Seen set spans chunks (it clears only at its own
        // bound), so every chunk carries new keys only.
        var results = await resolver.ResolveFirstPagesAsync(
            CyclingLookupChanges(count: 21, distinct: 6), pageSize: 100,
            (spec, _) => { chunks.Add(spec); return Task.CompletedTask; }, CancellationToken.None);

        chunks.Select(c => c.LookupValues.Count).ShouldBe([4, 2]);
        chunks.SelectMany(c => c.LookupValues).Select(t => t[0]).ShouldBe(
            Enumerable.Range(0, 6).Cast<object?>(), ignoreOrder: true);
        results.ShouldBeEmpty();
    }

    [Test]
    public async Task A_lookup_set_past_the_valve_degrades_to_a_whole_table_rebackfill()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var resolver = new DependentChangeResolver(
            dataSource, BuildModel(), instrumentation: null, maxKeysPerTransaction: 5, chunkSize: 2);
        var chunks = new List<ScopedFanoutSpec>();

        var results = await resolver.ResolveFirstPagesAsync(
            DistinctLookupChanges(10), pageSize: 100,
            (spec, _) => { chunks.Add(spec); return Task.CompletedTask; }, CancellationToken.None);

        var result = results.ShouldHaveSingleItem();
        result.RebackfillTable.ShouldNotBeNull();
        result.RebackfillTable.QualifiedName.ShouldBe("public.products");
        result.FirstPage.ShouldBeEmpty();

        // Chunks cut before the valve tripped stay queued: superseded by the whole-table run, harmless.
        chunks.Count.ShouldBe(2);
    }

    [Test]
    public async Task Without_a_queue_the_valve_still_degrades_to_a_whole_table_rebackfill()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var resolver = new DependentChangeResolver(
            dataSource, BuildModel(), instrumentation: null, maxKeysPerTransaction: 3, chunkSize: 2);

        // No enqueue callback: chunking is off, so only the valve bounds the set.
        var results = await resolver.ResolveFirstPagesAsync(
            DistinctLookupChanges(10), pageSize: 100, enqueueTail: null, CancellationToken.None);

        results.ShouldHaveSingleItem().RebackfillTable.ShouldNotBeNull();
    }

    [Test]
    public async Task An_update_repointing_the_lookup_fans_out_to_both_values()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var resolver = new DependentChangeResolver(
            dataSource, BuildModel(), instrumentation: null, maxKeysPerTransaction: 1_000_000, chunkSize: 2);
        var chunks = new List<ScopedFanoutSpec>();

        // One change whose old tuple carries a different lookup value: both scopes need refreshing
        // (the rows the lookup left behind would otherwise keep a stale copy).
        var results = await resolver.ResolveFirstPagesAsync(
            Changes(CategoryChange(id: 1, oldId: 2)), pageSize: 100,
            (spec, _) => { chunks.Add(spec); return Task.CompletedTask; }, CancellationToken.None);

        chunks.ShouldHaveSingleItem().LookupValues.Select(t => t[0]).ShouldBe([1, 2], ignoreOrder: true);
        results.ShouldBeEmpty();
    }

    [Test]
    public async Task An_update_with_an_unchanged_old_lookup_counts_the_key_once()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var resolver = new DependentChangeResolver(
            dataSource, BuildModel(), instrumentation: null, maxKeysPerTransaction: 1_000_000, chunkSize: 1);
        var chunks = new List<ScopedFanoutSpec>();

        await resolver.ResolveFirstPagesAsync(
            Changes(CategoryChange(id: 7, oldId: 7)), pageSize: 100,
            (spec, _) => { chunks.Add(spec); return Task.CompletedTask; }, CancellationToken.None);

        chunks.ShouldHaveSingleItem().LookupValues.ShouldHaveSingleItem()[0].ShouldBe(7);
    }

    [Test]
    public async Task An_update_without_old_lookup_values_logs_once_per_table()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=u;Password=p;Database=d");
        var collector = new FakeLogCollector();
        var resolver = new DependentChangeResolver(
            dataSource, BuildModel(), instrumentation: null, maxKeysPerTransaction: 1_000_000, chunkSize: 1,
            logger: new FakeLogger(collector));

        await resolver.ResolveFirstPagesAsync(
            Changes(CategoryChange(id: 1), CategoryChange(id: 2)), pageSize: 100,
            (_, _) => Task.CompletedTask, CancellationToken.None);

        var record = collector.GetSnapshot().ShouldHaveSingleItem();
        record.Level.ShouldBe(LogLevel.Debug);
        record.Message.ShouldContain("public.categories");
        record.Message.ShouldContain("REPLICA IDENTITY");
    }

    private static async IAsyncEnumerable<RawChange> Changes(params RawChange[] changes)
    {
        foreach (var change in changes)
        {
            yield return change;
        }
        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<RawChange> DistinctLookupChanges(int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return CategoryChange(id: i);
        }
        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<RawChange> CyclingLookupChanges(int count, int distinct)
    {
        for (var i = 0; i < count; i++)
        {
            yield return CategoryChange(id: i % distinct);
        }
        await Task.CompletedTask;
    }

    private static RawChange CategoryChange(int id, int? oldId = null) => new()
    {
        RelationId = 0,
        Schema = "public",
        TableName = "categories",
        Action = ChangeAction.Update,
        NewValues = [new RawColumn { ColumnName = "id", Value = id }],
        OldValues = oldId is null ? null : [new RawColumn { ColumnName = "id", Value = oldId.Value }],
    };

    private static WallabyModel BuildModel()
    {
        var products = Table("products", "id", "category_id");
        var categories = Table("categories", "id");
        return new WallabyModel(
            [products, categories],
            [
                new DependentBinding
                {
                    PrimaryTable = products,
                    DependentTable = categories,
                    Lookup = [new DependentLookupColumn("id", "category_id")],
                },
            ]);
    }

    private static CapturedTable Table(string name, params string[] columnNames)
    {
        var columns = columnNames
            .Select((c, i) => new CapturedColumn
            {
                PropertyName = c,
                ColumnName = c,
                ClrType = typeof(int),
                IsPrimaryKey = i == 0,
            })
            .ToList();
        return new CapturedTable
        {
            EntityClrType = typeof(object),
            Schema = "public",
            TableName = name,
            Columns = columns,
            PrimaryKey = [columns[0]],
        };
    }
}
