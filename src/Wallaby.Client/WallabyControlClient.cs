using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Client.Internal;

namespace Wallaby.Client;

/// <summary>
/// Remote control plane for a Wallaby installation, mediated entirely through its Postgres database — no
/// Wallaby host reference or running node required. Suspension durably drops every replication slot
/// Wallaby manages (primary and external) so platforms like RDS/Aurora can run a major-version upgrade,
/// and persists across restarts until <see cref="ResumeAsync"/>; on resume, Wallaby recreates its slots
/// and re-backfills every mapped table to converge sinks. Backfills can also be requested directly via
/// <see cref="RequestBackfillAsync"/>, addressed by schema-qualified table name.
/// </summary>
public sealed class WallabyControlClient : IAsyncDisposable
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    private readonly NpgsqlDataSource _dataSource;
    private readonly NpgsqlDataSource? _ownedDataSource;
    private readonly ILogger<WallabyControlClient> _logger;

    /// <summary>Create a client over an existing data source. The data source is not disposed with the client.</summary>
    public WallabyControlClient(NpgsqlDataSource dataSource, ILogger<WallabyControlClient>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource is NpgsqlMultiHostDataSource multiHost
            ? multiHost.WithTargetSession(TargetSessionAttributes.Primary)
            : dataSource;
        _logger = logger ?? NullLogger<WallabyControlClient>.Instance;
    }

    /// <summary>
    /// Create a client that owns a pooled data source built from <paramref name="connectionString"/>
    /// (the same database Wallaby is configured against), disposed with the client.
    /// </summary>
    public WallabyControlClient(string connectionString, ILogger<WallabyControlClient>? logger = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _ownedDataSource = NpgsqlDataSource.Create(connectionString);
        _dataSource = _ownedDataSource is NpgsqlMultiHostDataSource multiHost
            ? multiHost.WithTargetSession(TargetSessionAttributes.Primary)
            : _ownedDataSource;
        _logger = logger ?? NullLogger<WallabyControlClient>.Instance;
    }

    /// <summary>
    /// Suspend the Wallaby installation: persist the request, signal any running host, and (by default)
    /// wait until every managed replication slot is verified dropped. If no host acts within
    /// <see cref="WallabySuspendOptions.HostGracePeriod"/>, this client drops the slots itself. The
    /// suspension survives restarts and the database outage during an engine upgrade; it ends only with
    /// <see cref="ResumeAsync"/>. The client performs no DDL: the <c>wallaby.control</c> table is created
    /// by the Wallaby host, so a suspension-aware host must have run against the database at least once.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The database has no <c>wallaby.control</c> table — no suspension-aware Wallaby version has run
    /// against it.
    /// </exception>
    /// <exception cref="WallabyControlTimeoutException">
    /// The <see cref="WallabySuspendOptions.Timeout"/> expired before every slot was dropped — typically a
    /// consumer actively streaming from a managed slot (e.g. a Wallaby version without suspension support,
    /// or a third-party tool on an external slot). The request stays persisted.
    /// </exception>
    public async Task<WallabyControlState> SuspendAsync(
        WallabySuspendOptions? options = null, CancellationToken ct = default)
    {
        options ??= new WallabySuspendOptions();
        bool transitioned;
        try
        {
            transitioned = await ControlOperations.RequestSuspendAsync(
                _dataSource, ControlContract.OriginClient, options.Reason,
                options.RequestedBy ?? Environment.MachineName, ct);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            throw new InvalidOperationException(
                "This database has no wallaby.control table: no suspension-aware Wallaby version has run " +
                "against it. Deploy a Wallaby host with suspension support first — it creates the control " +
                "table at startup.", ex);
        }
        _logger.SuspendRequested(transitioned);

        if (!options.WaitForCompletion)
        {
            return await GetStateAsync(ct);
        }

        var start = Stopwatch.GetTimestamp();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(options.Timeout);
        var state = await GetStateAsync(ct);
        try
        {
            while (true)
            {
                state = await GetStateAsync(deadline.Token);
                options.Progress?.Report(state);
                if (state.State == WallabySuspensionState.Suspended && state.Slots.All(s => !s.ExistsOnServer))
                {
                    return state;
                }
                if (state.State == WallabySuspensionState.Running)
                {
                    // Resumed underneath us (another operator, or a host deployed without suspension in
                    // mind); report reality rather than fighting over the state.
                    return state;
                }

                // Past the grace period, any slot still (or newly) on the server is dropped from here —
                // covers no host running, a finalizer crash, and a slot recreated by an old-version node.
                if (Stopwatch.GetElapsedTime(start) >= options.HostGracePeriod)
                {
                    _logger.ClientFinalizing();
                    await ControlOperations.FinalizeSuspensionAsync(_dataSource, PollInterval, _logger, deadline.Token);
                    continue;
                }

                await Task.Delay(PollInterval, deadline.Token);
            }
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new WallabyControlTimeoutException(
                $"Suspension did not complete within {options.Timeout}. Last observed state: {state.State}; " +
                $"slots still on the server: {string.Join(", ", state.Slots.Where(s => s.ExistsOnServer).Select(s => s.SlotName))}. " +
                "An actively streamed slot cannot be dropped — stop its consumer (or upgrade Wallaby nodes to a " +
                "suspension-aware version) and retry; the request stays persisted.",
                state);
        }
    }

    /// <summary>
    /// End a suspension: Wallaby nodes wake, recreate their slots and publications, and re-backfill every
    /// mapped table (external-slot consumers must re-sync on their own). Returns once the state reads
    /// <see cref="WallabySuspensionState.Running"/> — slot recreation happens asynchronously on the next
    /// leader election; watch it via <see cref="GetStateAsync"/>. A no-op when nothing is suspended.
    /// Note: nodes deployed with the <c>Suspend()</c> builder flag re-assert their suspension — deploy
    /// them without the flag instead of resuming remotely.
    /// </summary>
    public async Task<WallabyControlState> ResumeAsync(CancellationToken ct = default)
    {
        var transitioned = await ControlOperations.ResumeAsync(_dataSource, configurationOriginOnly: false, ct);
        _logger.ResumeRequested(transitioned);
        return await GetStateAsync(ct);
    }

    /// <summary>
    /// Read the current control-plane state: suspension status and every managed slot joined with its
    /// live server state. Never requires DDL rights; a database without the control table reads as
    /// <see cref="WallabySuspensionState.Running"/>.
    /// </summary>
    public async Task<WallabyControlState> GetStateAsync(CancellationToken ct = default)
    {
        var row = await ControlOperations.ReadAsync(_dataSource, ct);
        var slots = await ControlOperations.ListManagedSlotsAsync(_dataSource, ct);
        return Map(row, slots);
    }

    /// <summary>
    /// Request a (re)backfill of <paramref name="tableQualifiedName"/> (schema-qualified, e.g.
    /// <c>public.orders</c>). The request is persisted — it survives restarts and is served by whichever
    /// node holds leadership, signalled instantly via LISTEN/NOTIFY. A request made while the table is
    /// already backfilling wins: the table re-runs from the start. A request for a table Wallaby does not
    /// capture stays <see cref="WallabyBackfillStatus.Requested"/> until a mapping for it deploys.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The database has no <c>wallaby.backfill_state</c> table — no Wallaby host has run against it.
    /// </exception>
    public async Task RequestBackfillAsync(string tableQualifiedName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableQualifiedName);
        try
        {
            await BackfillOperations.RequestAsync(_dataSource, tableQualifiedName, ct);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            throw new InvalidOperationException(
                "This database has no wallaby.backfill_state table: no Wallaby host has run against it. " +
                "The host creates it at startup.", ex);
        }
        _logger.BackfillRequested(tableQualifiedName);
    }

    /// <summary>
    /// The backfill state of every tracked table. Empty for a database no Wallaby host has run against.
    /// </summary>
    public async Task<IReadOnlyList<WallabyBackfillState>> GetBackfillStatusAsync(CancellationToken ct = default)
    {
        var rows = await BackfillOperations.ListStatesAsync(_dataSource, ct);
        return rows
            .Select(r => new WallabyBackfillState(
                r.TableQualified, Enum.Parse<WallabyBackfillStatus>(r.Status), r.RowsCopied, r.UpdatedAt))
            .ToList();
    }

    private static WallabyControlState Map(ControlRow? row, IReadOnlyList<ManagedSlotRow> slots)
    {
        var mapped = slots.Count == 0
            ? []
            : slots.Select(s => new WallabyManagedSlot(s.SlotName, s.Publication, s.Kind, s.ExistsOnServer, s.Active))
                .ToList() as IReadOnlyList<WallabyManagedSlot>;
        if (row is null)
        {
            return new WallabyControlState(
                WallabySuspensionState.Running, WallabySuspensionOrigin.Client,
                null, null, null, null, null, mapped);
        }

        var state = row.State switch
        {
            ControlContract.StateSuspendRequested => WallabySuspensionState.SuspendRequested,
            ControlContract.StateSuspended => WallabySuspensionState.Suspended,
            _ => WallabySuspensionState.Running,
        };
        var origin = row.Origin == ControlContract.OriginConfiguration
            ? WallabySuspensionOrigin.Configuration
            : WallabySuspensionOrigin.Client;
        return new WallabyControlState(
            state, origin, row.Reason, row.RequestedBy, row.RequestedAt, row.SuspendedAt, row.ResumedAt, mapped);
    }

    /// <summary>Disposes the data source when this client owns it (the connection-string constructor).</summary>
    public ValueTask DisposeAsync() => _ownedDataSource?.DisposeAsync() ?? ValueTask.CompletedTask;
}

/// <summary>Source-generated log messages for <see cref="WallabyControlClient"/>.</summary>
internal static partial class WallabyControlClientLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby suspension requested (transitioned={Transitioned}).")]
    internal static partial void SuspendRequested(this ILogger logger, bool transitioned);

    [LoggerMessage(Level = LogLevel.Information, Message = "No Wallaby host finalized the suspension within the grace period; dropping the managed slots from this client.")]
    internal static partial void ClientFinalizing(this ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby resume requested (transitioned={Transitioned}).")]
    internal static partial void ResumeRequested(this ILogger logger, bool transitioned);

    [LoggerMessage(Level = LogLevel.Information, Message = "Backfill requested for table '{Table}'.")]
    internal static partial void BackfillRequested(this ILogger logger, string table);
}
