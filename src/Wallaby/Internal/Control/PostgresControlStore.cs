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
    /// <summary>
    /// The control row; <c>null</c> (no row/table yet) means running. A control table predating this
    /// build's columns (an upgraded deployment reading before any leader bootstrapped) is healed by
    /// applying the pending schema steps and retrying, so every later operation in the same pass sees
    /// a current schema.
    /// </summary>
    public async Task<ControlRow?> ReadAsync(CancellationToken ct)
    {
        try
        {
            return await ControlOperations.ReadAsync(dataSource.Source, ct);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedColumn)
        {
            await EnsureStateSchemaAsync(ct);
            return await ControlOperations.ReadAsync(dataSource.Source, ct);
        }
    }

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
        await EnsureStateSchemaAsync(ct);
        return await ControlOperations.RequestSuspendAsync(
            dataSource.Source, ControlContract.OriginConfiguration, reason, Environment.MachineName, ct);
    }

    /// <summary>
    /// Refresh the configuration-assertion liveness heartbeat; called by a flag-carrying node on every
    /// gate pass while its suspension is in force, so flag-less nodes keep refusing the auto-resume.
    /// The gate reads the control row (healing an old schema) before it heartbeats, so the column
    /// exists by the time this runs.
    /// </summary>
    public Task HeartbeatConfigurationAssertionAsync(CancellationToken ct)
        => ControlOperations.HeartbeatConfigurationAssertionAsync(dataSource.Source, ct);

    private async Task EnsureStateSchemaAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.Source.OpenConnectionAsync(ct);
        await new StateSchemaBootstrapper(logger).EnsureAsync(connection, ct);
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
