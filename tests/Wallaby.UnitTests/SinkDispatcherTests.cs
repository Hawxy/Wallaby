using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Sinks;

namespace Wallaby.UnitTests;

public class SinkDispatcherTests
{
    private static readonly ChangeMetadata Meta = new("public", "products", DateTimeOffset.UtcNow, 1, 0, false);

    private static IReadOnlyList<RoutedDocument> OneRecord() =>
        [new RoutedDocument("sink", new SinkRecord(Destination: null, "1", Document: new WallabyDocument { ["x"] = 1 }, IsDeletion: false, Meta))];

    private static Dictionary<string, ISink> FailingSink() => new()
    {
        ["sink"] = new DelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Permanent("boom"))),
    };

    [Test]
    public async Task Permanent_failure_halts()
    {
        var dispatcher = new SinkDispatcher(FailingSink());

        await Should.ThrowAsync<Exception>(
            async () => await dispatcher.DispatchAsync(OneRecord(), CancellationToken.None));
    }

    [Test]
    public async Task Successful_delivery_passes_records_to_the_sink()
    {
        var received = 0;
        var sinks = new Dictionary<string, ISink>
        {
            ["sink"] = new DelegateSink("sink", (batch, _) =>
            {
                Interlocked.Add(ref received, batch.Records.Count);
                return Task.FromResult(DeliveryResult.Success);
            }),
        };

        await new SinkDispatcher(sinks).DispatchAsync(OneRecord(), CancellationToken.None);

        received.ShouldBe(1);
    }
}
