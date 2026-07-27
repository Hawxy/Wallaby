using Microsoft.Extensions.Logging;
using Npgsql;
using Wallaby.Client.Internal;
using Wallaby.DependencyInjection;
using Wallaby.Internal.State;

namespace Wallaby.Internal.Control;

/// <summary>
/// The host's view of the suspend/resume control plane: the shared <see cref="ControlOperations"/> SQL
/// bound to the instance's data source, plus a LISTEN subscription on the control channel.
/// </summary>
internal sealed class PostgresControlStore(WallabyDataSource dataSource, WallabyOptions options, ILogger logger)
{
    /// <summary>The control row; <c>null</c> (no row/table yet) means running.</summary>
    public Task<ControlRow?> ReadAsync(CancellationToken ct)
        => ControlOperations.ReadAsync(dataSource.Source, ct);

    /// <summary>True when the row exists and is not in the running state.</summary>
    public async Task<bool> IsSuspensionInEffectAsync(CancellationToken ct)
        => await ReadAsync(ct) is { } row && row.State != ControlContract.StateRunning;

    /// <summary>
    /// How stale the configuration-assertion heartbeat must be before a flag-less node auto-resumes.
    /// Flag-carrying nodes refresh the heartbeat every control poll, so four missed polls (or the
    /// configured floor, whichever is longer) means no live node is asserting the suspension.
    /// </summary>
    private TimeSpan AutoResumeGrace
    {
        get
        {
            var missedPolls = options.Advanced.ControlPollInterval * 4;
            var floor = options.Advanced.SuspensionAutoResumeGraceFloor;
            return missedPolls > floor ? missedPolls : floor;
        }
    }

    /// <summary>
    /// Assert the deployed <c>Suspend()</c> flag: ensure the state schema (the gate runs before a leader
    /// session can bootstrap it) and transition Running → SuspendRequested with the configuration origin.
    /// A suspension already in force (either origin) is left untouched.
    /// </summary>
    public async Task<bool> RequestConfigurationSuspendAsync(string? reason, CancellationToken ct)
    {
        await using (var connection = await dataSource.Source.OpenConnectionAsync(ct))
        {
            await new StateSchemaBootstrapper(logger).EnsureAsync(connection, ct);
        }
        return await ControlOperations.RequestSuspendAsync(
            dataSource.Source, ControlContract.OriginConfiguration, reason, Environment.MachineName, ct);
    }

    /// <summary>
    /// Refresh the configuration-assertion liveness heartbeat; called by a flag-carrying node on every
    /// gate pass while its suspension is in force, so flag-less nodes keep refusing the auto-resume.
    /// </summary>
    public async Task HeartbeatConfigurationAssertionAsync(CancellationToken ct)
    {
        try
        {
            await ControlOperations.HeartbeatConfigurationAssertionAsync(dataSource.Source, ct);
        }
        catch (PostgresException ex) when (ex.SqlState == "42703")
        {
            // The control row was written by an older host and the heartbeat column is missing; apply
            // the pending schema steps and retry.
            await using (var connection = await dataSource.Source.OpenConnectionAsync(ct))
            {
                await new StateSchemaBootstrapper(logger).EnsureAsync(connection, ct);
            }
            await ControlOperations.HeartbeatConfigurationAssertionAsync(dataSource.Source, ct);
        }
    }

    /// <summary>
    /// Auto-resume for a node deployed without the <c>Suspend()</c> flag: ends a configuration-origin
    /// suspension only, and only once its assertion heartbeat has been quiet for the grace window. A
    /// client-requested suspension stays in force until an explicit remote resume, and a mixed rolling
    /// deployment (flag pods still alive) doesn't flap slots.
    /// </summary>
    public Task<bool> ResumeConfigurationSuspensionAsync(CancellationToken ct)
        => ControlOperations.ResumeAsync(dataSource.Source, configurationOriginOnly: true, ct, AutoResumeGrace);

    /// <summary>Drop every managed slot still on the server and mark the suspension finalized.</summary>
    public Task<bool> FinalizeSuspensionAsync(TimeSpan busyRetryDelay, CancellationToken ct)
        => ControlOperations.FinalizeSuspensionAsync(dataSource.Source, busyRetryDelay, logger, ct);

    /// <summary>A LISTEN subscription that wakes on any control-state transition.</summary>
    public INotifySubscription Subscribe()
        => new PostgresChannelSubscription(dataSource.Source, WallabySchema.ControlNotifyChannel);
}
