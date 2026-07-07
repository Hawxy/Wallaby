using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Internal.Pipeline;
using Wallaby.Providers;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit.OpenTelemetry;

public class MappingChangeRouterTelemetryTests
{
    /// <summary>A transform that emits one document per change without touching the session.</summary>
    private sealed class StubTransform : IWallabyTransformInvoker
    {
        public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
            object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
        {
            var documents = new Dictionary<DocumentKey, WallabyDocument?>();
            foreach (var change in changes)
            {
                documents[change.Key] = new WallabyDocument { ["key"] = change.Key.ToString() };
            }
            return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
        }
    }

    [Test]
    public async Task Transform_invocation_records_duration_and_a_span_per_entity()
    {
        var instr = new WallabyInstrumentation();
        using var duration = new MetricCollector<double>(instr.Meter, "wallaby.transform.duration");

        Activity? captured = null;
        using var listener = new ActivityListener
        {
            // Scope to THIS instrumentation's source instance — ActivityListeners are process-global and
            // match by name, so a name filter would capture transform spans from tests running in parallel.
            ShouldListenTo = source => ReferenceEquals(source, instr.ActivitySource),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.OperationName == "transform")
                {
                    captured = activity;
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        // The transform ignores the context, so a never-opened context is sufficient for a unit test.
        var sessionProvider = new DbContextEnrichmentSessionProvider(
            () => new AppDbContext(TestModelFactory.CreateOptions("Host=localhost;Username=u;Password=p;Database=d")));
        var mapping = new EntityMapping
        {
            EntityClrType = typeof(Product),
            SinkName = "sink",
            Destination = "products",
            Transform = new StubTransform(),
            Sessions = sessionProvider,
        };
        var router = new MappingChangeRouter([mapping], instr);

        var meta = new ChangeMetadata("public", "products", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, false);
        var change = new ChangeEvent(
            ChangeAction.Insert, meta, new Product { Id = 1, Name = "a" },
            new Dictionary<string, object?>(), Changes: null, new object[] { 1 })
        {
            EntityClrType = typeof(Product),
        };

        var routed = await router.RouteAsync([change], CancellationToken.None);

        routed.Count.ShouldBe(1);
        var measurement = duration.GetMeasurementSnapshot().ShouldHaveSingleItem();
        measurement.Tags["wallaby.entity"].ShouldBe("Product");
        measurement.Tags["wallaby.sink"].ShouldBe("sink");
        captured.ShouldNotBeNull();
        captured!.GetTagItem("wallaby.entity").ShouldBe("Product");
        captured.GetTagItem("wallaby.sink").ShouldBe("sink");
    }
}
