using Microsoft.Extensions.Logging;
using Wallaby.Client.Internal;

namespace Wallaby.Internal.Control;

/// <summary>What the control state requires of a node before it may provision slots or stream.</summary>
internal enum ControlGateAction
{
    /// <summary>No suspension in effect — provision/stream normally.</summary>
    Proceed,

    /// <summary>A suspension is requested but not finalized: drop the managed slots (under the cluster lock).</summary>
    Finalize,

    /// <summary>The installation is suspended: idle on the control channel until resumed.</summary>
    Idle,
}

/// <summary>
/// Evaluates the suspend/resume control gate every hosted service passes before touching slots,
/// reconciling the deployed <c>Suspend()</c> flag with the durable control row: the flag asserts a
/// configuration-origin suspension (over a remote resume) and its absence auto-resumes one — while a
/// client-origin suspension is never auto-resumed.
/// </summary>
internal static class ControlGateEvaluator
{
    public static async Task<(ControlGateAction Action, ControlRow? Row)> EvaluateAsync(
        PostgresControlStore store, bool suspendedFlag, string? suspensionReason, ILogger logger, CancellationToken ct)
    {
        var row = await store.ReadAsync(ct);
        var state = row?.State ?? ControlContract.StateRunning;

        if (suspendedFlag)
        {
            if (state == ControlContract.StateRunning)
            {
                await store.RequestConfigurationSuspendAsync(suspensionReason, ct);
                logger.ConfigurationSuspendAsserted();
                row = await store.ReadAsync(ct);
                state = row?.State ?? ControlContract.StateSuspendRequested;
            }
        }
        else if (row is not null && state != ControlContract.StateRunning &&
                 row.Origin == ControlContract.OriginConfiguration)
        {
            // Deployed without the flag: the flag-driven suspension has served its purpose (the upgrade
            // window); resume and proceed. A concurrent re-suspend is caught by the session's own checks.
            if (await store.ResumeConfigurationSuspensionAsync(ct))
            {
                logger.ConfigurationSuspensionAutoResumed();
            }
            return (ControlGateAction.Proceed, row);
        }

        return state switch
        {
            ControlContract.StateSuspendRequested => (ControlGateAction.Finalize, row),
            ControlContract.StateSuspended => (ControlGateAction.Idle, row),
            _ => (ControlGateAction.Proceed, row),
        };
    }
}

/// <summary>Source-generated log messages for <see cref="ControlGateEvaluator"/>.</summary>
internal static partial class ControlGateEvaluatorLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "This node is deployed with Suspend(): requesting installation-wide suspension (managed replication slots will be dropped).")]
    internal static partial void ConfigurationSuspendAsserted(this ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Deployed without Suspend(): auto-resuming the configuration-driven suspension.")]
    internal static partial void ConfigurationSuspensionAutoResumed(this ILogger logger);
}
