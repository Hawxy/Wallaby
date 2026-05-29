using EFCore.CDC.Abstractions;
using EFCore.CDC.Internal.Materialization;
using EFCore.CDC.Internal.Pipeline;
using EFCore.CDC.Internal.Replication;
using EFCore.CDC.Internal.State;
using EFCore.CDC.TestModel;
using Microsoft.Extensions.Logging.Abstractions;

namespace EFCore.CDC.Meilisearch.IntegrationTests;

/// <summary>Runs a <see cref="CdcPipeline"/> in the background until a predicate holds (or a timeout elapses).</summary>
internal static class PipelineDriver
{
    public static async Task RunUntilAsync(
        string connectionString,
        string slot,
        string publication,
        IChangeRouter router,
        IReadOnlyDictionary<string, ISink> sinks,
        Func<Task<bool>> until,
        TimeSpan timeout)
    {
        var stream = new LogicalReplicationStream(connectionString, slot, publication);
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        var factory = new ChangeEventFactory(new EntityMaterializer(ctx.Model));
        var dispatcher = new SinkDispatcher(sinks);
        var pipeline = new CdcPipeline(
            stream, factory, router, dispatcher, new PostgresCheckpointStore(connectionString), slot, NullLogger.Instance);

        using var cts = new CancellationTokenSource();
        var run = Task.Run(() => pipeline.RunAsync(cts.Token));
        Exception? error = null;
        var deadline = DateTime.UtcNow + timeout;
        try
        {
            while (DateTime.UtcNow < deadline)
            {
                if (run.IsFaulted) { error = run.Exception?.GetBaseException(); break; }
                if (await until()) break;
                await Task.Delay(250);
            }
        }
        finally
        {
            await cts.CancelAsync();
            try { await run; } catch (Exception ex) when (ex is not OperationCanceledException) { error ??= ex; } catch { }
            await stream.DisposeAsync();
        }

        if (error is not null)
        {
            throw new Exception("CDC pipeline faulted: " + error, error);
        }
    }
}
