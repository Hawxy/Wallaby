using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;

namespace Wallaby.Tests.Unit;

public class SinkDisposalTests
{
    [Test]
    public async Task Async_disposable_sinks_are_disposed()
    {
        var sink = new AsyncDisposableSink("a");

        await SinkDisposal.DisposeAllAsync([sink], new FakeLogger());

        sink.AsyncDisposed.ShouldBe(1);
    }

    [Test]
    public async Task Sync_disposable_sinks_are_disposed()
    {
        var sink = new SyncDisposableSink("a");

        await SinkDisposal.DisposeAllAsync([sink], new FakeLogger());

        sink.Disposed.ShouldBe(1);
    }

    [Test]
    public async Task A_sink_implementing_both_is_disposed_asynchronously_only()
    {
        var sink = new DualDisposableSink("a");

        await SinkDisposal.DisposeAllAsync([sink], new FakeLogger());

        sink.AsyncDisposed.ShouldBe(1);
        sink.Disposed.ShouldBe(0);
    }

    [Test]
    public async Task Non_disposable_sinks_are_skipped()
    {
        await SinkDisposal.DisposeAllAsync([new PlainSink("a")], new FakeLogger());
    }

    [Test]
    public async Task A_throwing_dispose_is_logged_and_the_remaining_sinks_are_still_disposed()
    {
        var throwing = new ThrowingDisposableSink("bad");
        var after = new AsyncDisposableSink("good");
        var logger = new FakeLogger();

        await SinkDisposal.DisposeAllAsync([throwing, after], logger);

        after.AsyncDisposed.ShouldBe(1);
        var warning = logger.Collector.GetSnapshot().ShouldHaveSingleItem();
        warning.Level.ShouldBe(LogLevel.Warning);
        warning.Message.ShouldContain("bad");
    }

    private class PlainSink(string name) : ISink
    {
        public string Name { get; } = name;

        public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct) =>
            Task.FromResult(DeliveryResult.Success);
    }

    private sealed class AsyncDisposableSink(string name) : PlainSink(name), IAsyncDisposable
    {
        public int AsyncDisposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            AsyncDisposed++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class SyncDisposableSink(string name) : PlainSink(name), IDisposable
    {
        public int Disposed { get; private set; }

        public void Dispose() => Disposed++;
    }

    private sealed class DualDisposableSink(string name) : PlainSink(name), IAsyncDisposable, IDisposable
    {
        public int AsyncDisposed { get; private set; }
        public int Disposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            AsyncDisposed++;
            return ValueTask.CompletedTask;
        }

        public void Dispose() => Disposed++;
    }

    private sealed class ThrowingDisposableSink(string name) : PlainSink(name), IAsyncDisposable
    {
        public ValueTask DisposeAsync() => throw new InvalidOperationException("dispose failed");
    }
}
