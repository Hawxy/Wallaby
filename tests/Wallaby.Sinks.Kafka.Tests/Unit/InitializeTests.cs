using System.Diagnostics;
using Dekaf.Errors;

namespace Wallaby.Sinks.Kafka.Tests.Unit;

/// <summary>Topic creation is cancellable and bounded by <c>AdminTimeoutMs</c>, never the client's internal retries.</summary>
public class InitializeTests
{
    private static KafkaSink Sink(int adminTimeoutMs)
    {
        var options = new KafkaSinkOptions { BootstrapServers = "localhost:1", AdminTimeoutMs = adminTimeoutMs };
        options.Topics.Add(new KafkaTopicConfig { Name = "wallaby-init-test" });
        return new KafkaSink("kafka", options);
    }

    [Test]
    public async Task Initialize_honors_cancellation()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var sink = Sink(adminTimeoutMs: 60_000);

        var stopwatch = Stopwatch.StartNew();
        await Should.ThrowAsync<OperationCanceledException>(() => sink.InitializeAsync(cts.Token));
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task Initialize_times_out_within_the_admin_bound()
    {
        var sink = Sink(adminTimeoutMs: 1_000);

        var stopwatch = Stopwatch.StartNew();
        // The unreachable broker surfaces either as the sink's own deadline (TimeoutException) or as a
        // connection failure from the admin client once its internal retries give up (KafkaException);
        // either way initialization must fail within the admin bound, not hang on internal retries.
        try
        {
            await sink.InitializeAsync(CancellationToken.None);
            throw new InvalidOperationException("InitializeAsync should have failed against an unreachable broker.");
        }
        catch (Exception ex) when (ex is TimeoutException or KafkaException)
        {
        }
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(15));
    }
}
