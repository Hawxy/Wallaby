using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Diagnostics;
using Wallaby.Internal.Pipeline;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Unit.OpenTelemetry;

public class MappingChangeRouterTelemetryTests
{
    private sealed class Doc;

    [Test]
    public async Task Transform_invocation_records_duration_and_a_span_per_entity()
    {
        var instr = new WallabyInstrumentation();
        using var duration = new MetricCollector<double>(instr.Meter, "wallaby.transform.duration");
        using var activities = new ActivityCapture(instr);

        var router = new MappingChangeRouter(
            [TestChanges.Mapping(typeof(Doc), new RecordingTransform(), new FakeSessionProvider())], instr);

        var routed = await router.RouteAsync([TestChanges.Change(typeof(Doc), 1)], CancellationToken.None);

        routed.Count.ShouldBe(1);
        var measurement = duration.GetMeasurementSnapshot().ShouldHaveSingleItem();
        measurement.Tags["wallaby.entity"].ShouldBe("Doc");
        measurement.Tags["wallaby.sink"].ShouldBe("sink");
        var captured = activities.Last("transform");
        captured.ShouldNotBeNull();
        captured!.GetTagItem("wallaby.entity").ShouldBe("Doc");
        captured.GetTagItem("wallaby.sink").ShouldBe("sink");
    }
}
