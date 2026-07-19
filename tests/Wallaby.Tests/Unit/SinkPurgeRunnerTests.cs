using Microsoft.Extensions.Logging.Abstractions;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Model;

namespace Wallaby.Tests.Unit;

public class SinkPurgeRunnerTests
{
    private static readonly CapturedTable Products = new()
    {
        EntityClrType = typeof(object),
        Schema = "public",
        TableName = "products",
        Columns = [],
        PrimaryKey = [],
    };

    private sealed class PlainSink(string name) : ISink
    {
        public string Name { get; } = name;

        public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
            => Task.FromResult(DeliveryResult.Success);
    }

    private sealed class PurgerSink(string name, Exception? throwOnPurge = null) : ISink, ISinkPurger
    {
        public string Name { get; } = name;
        public List<SinkPurgeRequest> Purged { get; } = [];

        public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
            => Task.FromResult(DeliveryResult.Success);

        public Task PurgeAsync(SinkPurgeRequest request, CancellationToken ct)
        {
            if (throwOnPurge is not null)
            {
                throw throwOnPurge;
            }
            Purged.Add(request);
            return Task.CompletedTask;
        }
    }

    private static SinkPurgeRunner Runner(params ISink[] sinks)
        => new(sinks.ToDictionary(s => s.Name), WallabyInstrumentation.NoOp, NullLogger.Instance);

    private static BackfillTable Table(params SinkPurgeTarget[] targets)
        => new(Products, TransformVersion: null, PurgeOnVersionChange: false, targets);

    [Test]
    public async Task The_request_carries_the_table_and_destination()
    {
        var sink = new PurgerSink("search");

        await Runner(sink).PurgeAsync(Table(new SinkPurgeTarget("search", "products-index", Scoped: false)), default);

        var request = sink.Purged.ShouldHaveSingleItem();
        request.TableSchema.ShouldBe("public");
        request.TableName.ShouldBe("products");
        request.QualifiedTableName.ShouldBe("public.products");
        request.Destination.ShouldBe("products-index");
    }

    [Test]
    public async Task A_null_destination_passes_through_as_the_sink_default()
    {
        var sink = new PurgerSink("search");

        await Runner(sink).PurgeAsync(Table(new SinkPurgeTarget("search", Destination: null, Scoped: false)), default);

        sink.Purged.ShouldHaveSingleItem().Destination.ShouldBeNull();
    }

    [Test]
    public async Task A_sink_without_the_capability_is_skipped_and_the_rest_still_purge()
    {
        var plain = new PlainSink("audit");
        var purger = new PurgerSink("search");

        await Runner(plain, purger).PurgeAsync(
            Table(
                new SinkPurgeTarget("audit", "product-log", Scoped: false),
                new SinkPurgeTarget("search", "products-index", Scoped: false)),
            default);

        purger.Purged.ShouldHaveSingleItem().Destination.ShouldBe("products-index");
    }

    [Test]
    public async Task A_scoped_target_is_skipped_and_a_fixed_target_on_the_same_table_still_purges()
    {
        var sink = new PurgerSink("search");

        await Runner(sink).PurgeAsync(
            Table(
                new SinkPurgeTarget("search", Destination: null, Scoped: true),
                new SinkPurgeTarget("search", "products-index", Scoped: false)),
            default);

        sink.Purged.ShouldHaveSingleItem().Destination.ShouldBe("products-index");
    }

    [Test]
    public async Task A_purge_failure_propagates_to_fail_the_backfill_run()
    {
        var sink = new PurgerSink("search", throwOnPurge: new InvalidOperationException("index locked"));

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => Runner(sink)
            .PurgeAsync(Table(new SinkPurgeTarget("search", "products-index", Scoped: false)), default));
        ex.Message.ShouldBe("index locked");
    }
}
