using System.Collections.Concurrent;
using System.Diagnostics;
using Wallaby.Diagnostics;

namespace Wallaby.TestInfrastructure;

/// <summary>
/// Records every activity completed by one <see cref="WallabyInstrumentation"/> instance.
/// <see cref="ActivityListener"/>s are process-global and match by source name, so this scopes by
/// instance identity — a name filter would capture spans from tests running in parallel.
/// </summary>
public sealed class ActivityCapture : IDisposable
{
    private readonly ConcurrentQueue<Activity> _stopped = new();
    private readonly ActivityListener _listener;

    public ActivityCapture(WallabyInstrumentation instrumentation)
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, instrumentation.ActivitySource),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = _stopped.Enqueue,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    /// <summary>Operation names of the completed activities, in completion order.</summary>
    public IReadOnlyList<string> OperationNames => [.. _stopped.Select(a => a.OperationName)];

    /// <summary>The most recently completed activity with the given operation name, or null.</summary>
    public Activity? Last(string operationName) => _stopped.LastOrDefault(a => a.OperationName == operationName);

    public void Dispose() => _listener.Dispose();
}
