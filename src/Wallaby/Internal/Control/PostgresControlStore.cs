using Microsoft.Extensions.Logging;
using Wallaby.Client.Internal;
using Wallaby.Internal.State;

namespace Wallaby.Internal.Control;

/// <summary>
/// The host's view of the suspend/resume control plane: the shared <see cref="ControlOperations"/> SQL
/// bound to the instance's data source, plus a LISTEN subscription on the control channel.
/// </summary>
internal sealed class PostgresControlStore(WallabyDataSource dataSource, ILogger logger)
{
    /// <summary>The control row; <c>null</c> (no row/table yet) means running.</summary>
    public Task<ControlRow?> ReadAsync(CancellationToken ct)
        => ControlOperations.ReadAsync(dataSource.Source, ct);

    /// <summary>True when the row exists and is not in the running state.</summary>
    public async Task<bool> IsSuspensionInEffectAsync(CancellationToken ct)
        => await ReadAsync(ct) is { } row && row.State != ControlContract.StateRunning;

    /// <summary>
    /// Assert the deployed <c>Suspend()</c> flag: ensure the control table and transition Running →
    /// SuspendRequested with the configuration origin. A suspension already in force (either origin)
    /// is left untouched.
    /// </summary>
    public async Task<bool> RequestConfigurationSuspendAsync(string? reason, CancellationToken ct)
    {
        await ControlOperations.EnsureControlTableAsync(dataSource.Source, ct);
        return await ControlOperations.RequestSuspendAsync(
            dataSource.Source, ControlContract.OriginConfiguration, reason, Environment.MachineName, ct);
    }

    /// <summary>
    /// Auto-resume for a node deployed without the <c>Suspend()</c> flag: ends a configuration-origin
    /// suspension only — a client-requested one stays in force until an explicit remote resume.
    /// </summary>
    public Task<bool> ResumeConfigurationSuspensionAsync(CancellationToken ct)
        => ControlOperations.ResumeAsync(dataSource.Source, configurationOriginOnly: true, ct);

    /// <summary>Drop every managed slot still on the server and mark the suspension finalized.</summary>
    public Task<bool> FinalizeSuspensionAsync(TimeSpan busyRetryDelay, CancellationToken ct)
        => ControlOperations.FinalizeSuspensionAsync(dataSource.Source, busyRetryDelay, logger, ct);

    /// <summary>A LISTEN subscription that wakes on any control-state transition.</summary>
    public INotifySubscription Subscribe()
        => new PostgresChannelSubscription(dataSource.Source, WallabySchema.ControlNotifyChannel);
}
