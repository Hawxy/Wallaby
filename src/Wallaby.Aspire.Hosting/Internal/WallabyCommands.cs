using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Wallaby.Client;

namespace Wallaby.Aspire.Hosting.Internal;

/// <summary>The dashboard commands of a <see cref="WallabyResource"/>, each a thin call into <see cref="WallabyControlClient"/>.</summary>
internal static class WallabyCommands
{
    public const string Backfill = "wallaby-backfill";
    public const string BackfillAll = "wallaby-backfill-all";
    public const string CancelBackfill = "wallaby-cancel-backfill";
    public const string Suspend = "wallaby-suspend";
    public const string Resume = "wallaby-resume";
    public const string Widen = "wallaby-widen-publications";
    public const string Restore = "wallaby-restore-publications";

    private const string TableArgument = "table";
    private const string PurgeArgument = "purge";
    private const string ReasonArgument = "reason";
    private const string RequestedBy = "Aspire dashboard";

    public static void Add(IResourceBuilder<WallabyResource> builder)
    {
        var monitor = builder.Resource.Monitor;

        builder.WithCommand(Backfill, "Backfill table",
            ctx => RunAsync(monitor, ctx, async (client, ct) =>
            {
                var table = Table(ctx);
                var purge = Flag(ctx, PurgeArgument);
                await client.RequestBackfillAsync(table, purge, ct);
                return $"Backfill of {table} requested{PurgeSuffix(purge)}.";
            }),
            new CommandOptions
            {
                Description = "Re-snapshot one table into every sink it maps to, optionally purging its destinations first.",
                IconName = "ArrowSync",
                IsHighlighted = true,
                Arguments = [TableInput(monitor, "Table to backfill"), PurgeInput()],
                ValidateArguments = ValidateTable,
                UpdateState = _ => Enabled(WallabyObservationMapper.CanBackfill(monitor)),
            });

        builder.WithCommand(BackfillAll, "Backfill all tracked tables",
            ctx => RunAsync(monitor, ctx, async (client, ct) =>
            {
                var purge = Flag(ctx, PurgeArgument);
                var tables = (await client.GetBackfillStatusAsync(ct)).Select(b => b.Table).ToList();
                foreach (var table in tables)
                {
                    await client.RequestBackfillAsync(table, purge, ct);
                }
                return $"Backfill requested for {tables.Count} table(s){PurgeSuffix(purge)}.";
            }),
            new CommandOptions
            {
                Description = "Re-snapshot every table Wallaby has backfilled before.",
                IconName = "ArrowSyncCircle",
                Arguments = [PurgeInput()],
                UpdateState = _ => Enabled(WallabyObservationMapper.CanBackfillAll(monitor)),
            });

        builder.WithCommand(CancelBackfill, "Cancel backfill",
            ctx => RunAsync(monitor, ctx, async (client, ct) =>
            {
                var table = Table(ctx);
                return await client.CancelBackfillAsync(table, ct)
                    ? $"Queued backfill of {table} cancelled."
                    : $"{table} has no queued backfill; a running backfill can't be cancelled.";
            }),
            new CommandOptions
            {
                Description = "Withdraw a backfill request the leader hasn't started yet.",
                IconName = "DismissCircle",
                Arguments = [TableInput(monitor, "Table whose queued backfill to cancel", WallabyBackfillStatus.Requested)],
                ValidateArguments = ValidateTable,
                UpdateState = _ => Enabled(WallabyObservationMapper.CanCancelBackfill(monitor)),
            });

        builder.WithCommand(Suspend, "Suspend",
            ctx => RunAsync(monitor, ctx, async (client, ct) =>
            {
                var reason = ctx.Arguments.GetString(ReasonArgument);
                await client.SuspendAsync(new WallabySuspendOptions
                {
                    Reason = string.IsNullOrWhiteSpace(reason) ? null : reason,
                    RequestedBy = RequestedBy,
                }, ct);
                return "Wallaby is suspended: every managed replication slot is dropped.";
            }),
            new CommandOptions
            {
                Description = "Drop every managed replication slot and stop replication until resumed. Resuming re-backfills every table.",
                IconName = "Pause",
                Arguments =
                [
                    new InteractionInput { Name = ReasonArgument, Label = "Reason", InputType = InputType.Text, Required = false },
                ],
                Progress = new CommandProgressOptions { Title = "Suspending Wallaby", Message = "Waiting for every managed slot to be dropped." },
                UpdateState = _ => Enabled(WallabyObservationMapper.CanSuspend(monitor)),
            });

        builder.WithCommand(Resume, "Resume",
            ctx => RunAsync(monitor, ctx, async (client, ct) =>
            {
                var purge = Flag(ctx, PurgeArgument);
                await client.ResumeAsync(purge, ct);
                return "Wallaby resumed; the leader recreates its slots and re-backfills.";
            }),
            new CommandOptions
            {
                Description = "End a suspension. The next leader recreates the slots and re-backfills every table.",
                IconName = "Play",
                Arguments = [PurgeInput()],
                UpdateState = _ => Enabled(WallabyObservationMapper.CanResume(monitor)),
            });

        builder.WithCommand(Widen, "Widen publications",
            ctx => RunAsync(monitor, ctx, async (client, ct) =>
            {
                await client.WidenPublicationsAsync(new WallabyWidenOptions { RequestedBy = RequestedBy }, ct);
                return "Publications widened: column-listed tables accept ALTER COLUMN TYPE until restored.";
            }),
            new CommandOptions
            {
                Description = "Temporarily publish whole tables so migrations refused by publication column lists can run.",
                IconName = "ArrowExpand",
                Progress = new CommandProgressOptions { Title = "Widening publications", Message = "Waiting for every managed publication to be widened." },
                UpdateState = _ => Enabled(WallabyObservationMapper.CanWiden(monitor)),
            });

        builder.WithCommand(Restore, "Restore publications",
            ctx => RunAsync(monitor, ctx, async (client, ct) =>
            {
                await client.RestorePublicationsAsync(ct);
                return "Restore requested; the next leader term narrows the publications again.";
            }),
            new CommandOptions
            {
                Description = "End a widening; the host narrows its publications again.",
                IconName = "ArrowMinimize",
                UpdateState = _ => Enabled(WallabyObservationMapper.CanRestore(monitor)),
            });
    }

    private static InteractionInput TableInput(
        WallabyMonitorState monitor, string label, WallabyBackfillStatus? onlyStatus = null) => new()
    {
        Name = TableArgument,
        Label = label,
        Description = "Schema-qualified, e.g. public.orders.",
        InputType = InputType.Choice,
        Required = true,
        AllowCustomChoice = true,
        DynamicLoading = new InputLoadOptions
        {
            AlwaysLoadOnStart = true,
            LoadCallback = async ctx =>
            {
                if (monitor.Client is not { } client)
                {
                    return;
                }
                var tables = await client.GetBackfillStatusAsync(ctx.CancellationToken);
                ctx.Input.Options = tables
                    .Where(b => onlyStatus is null || b.Status == onlyStatus)
                    .Select(b => KeyValuePair.Create(b.Table, b.Table))
                    .ToList();
            },
        },
    };

    private static InteractionInput PurgeInput() => new()
    {
        Name = PurgeArgument,
        Label = "Purge destinations first",
        Description = "Delete the existing documents before re-snapshotting, so rows deleted while CDC was down don't linger.",
        InputType = InputType.Boolean,
        Value = "false",
    };

    private static Task ValidateTable(InputsDialogValidationContext ctx)
    {
        var table = ctx.Inputs.GetString(TableArgument)?.Trim();
        if (string.IsNullOrEmpty(table) || table.Split('.') is not [{ Length: > 0 }, { Length: > 0 }])
        {
            ctx.AddValidationError(TableArgument, "Enter a schema-qualified table name, e.g. public.orders.");
        }
        return Task.CompletedTask;
    }

    // API and CLI callers may omit an optional flag, which leaves the input without a value.
    private static bool Flag(ExecuteCommandContext ctx, string name)
        => bool.TryParse(ctx.Arguments.GetString(name), out var value) && value;

    private static string Table(ExecuteCommandContext ctx) => ctx.Arguments.GetString(TableArgument)!.Trim();

    private static string PurgeSuffix(bool purge) => purge ? " (purge first)" : string.Empty;

    private static ResourceCommandState Enabled(bool enabled)
        => enabled ? ResourceCommandState.Enabled : ResourceCommandState.Disabled;

    private static async Task<ExecuteCommandResult> RunAsync(
        WallabyMonitorState monitor,
        ExecuteCommandContext ctx,
        Func<WallabyControlClient, CancellationToken, Task<string>> action)
    {
        if (monitor.Client is not { } client)
        {
            return CommandResults.Failure("The Wallaby monitor isn't connected to the database yet.");
        }
        try
        {
            var message = await action(client, ctx.CancellationToken);
            return CommandResults.Success(message);
        }
        catch (OperationCanceledException) when (ctx.CancellationToken.IsCancellationRequested)
        {
            return CommandResults.Canceled();
        }
        catch (WallabySchemaVersionException)
        {
            return CommandResults.Failure(
                "No current Wallaby host has run against this database yet; start the worker running Wallaby first.");
        }
        catch (WallabyControlTimeoutException ex)
        {
            return CommandResults.Failure(
                $"{ex.Message} Last observed state: {WallabyObservationMapper.Describe(ex.LastObservedState.State)}.");
        }
        catch (Exception ex)
        {
            return CommandResults.Failure(ex);
        }
        finally
        {
            monitor.PollNow();
        }
    }
}
