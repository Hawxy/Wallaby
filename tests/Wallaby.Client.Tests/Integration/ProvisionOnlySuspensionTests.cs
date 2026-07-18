using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;

namespace Wallaby.Client.Tests.Integration;

/// <summary>
/// A provision-only host (external slots, no capture) must honor a suspension instead of recreating the
/// external slots underneath the upgrade, and provision once resumed.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class ProvisionOnlySuspensionTests(PostgresFixture pg)
{
    [Test]
    public async Task Provision_only_host_waits_out_a_suspension_before_creating_slots()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var table = $"ext_target_{suffix}";
        var slot = $"elt_slot_{suffix}";
        var publication = $"elt_pub_{suffix}";
        await ExecAsync($"CREATE TABLE {table} (id int PRIMARY KEY)");

        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            // The client performs no DDL — simulate a suspension-aware host having run once.
            await ExecAsync(
                """
                CREATE SCHEMA IF NOT EXISTS wallaby;
                CREATE TABLE IF NOT EXISTS wallaby.control (
                    scope        text        PRIMARY KEY DEFAULT 'wallaby' CHECK (scope = 'wallaby'),
                    state        text        NOT NULL DEFAULT 'Running',
                    origin       text        NOT NULL DEFAULT 'client',
                    reason       text        NULL,
                    requested_by text        NULL,
                    requested_at timestamptz NULL,
                    suspended_at timestamptz NULL,
                    resumed_at   timestamptz NULL,
                    updated_at   timestamptz NOT NULL DEFAULT now()
                );
                """);

            // Suspend before the host exists (nothing to drop — finalizes instantly from the client).
            var suspended = await client.SuspendAsync(new WallabySuspendOptions
            {
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(30),
            });
            suspended.State.ShouldBe(WallabySuspensionState.Suspended);

            var services = new ServiceCollection();
            services.AddLogging();
            services.AddWallaby(cdc => cdc
                .UseConnectionString(pg.ConnectionString)
                .AddExternalSlot(slot, e => e.WithPublication(publication).ForTable("public", table)));
            services.ConfigureWallabyOptions(o => o.Advanced.ControlPollInterval = TimeSpan.FromMilliseconds(500));

            await using var node = await WallabyTestNode.StartAsync(services);
            var status = node.Services.GetRequiredService<IWallabyStatus>();

            await PollUntilAsync(
                () => Task.FromResult(status.Current.Role == WallabyNodeRole.Suspended),
                "provision-only node to report Suspended");
            (await SlotExistsAsync(slot)).ShouldBeFalse();

            await client.ResumeAsync();

            await PollUntilAsync(() => SlotExistsAsync(slot), "external slot to be provisioned after resume");
            status.Current.Faulted.ShouldBeFalse();
        }
        finally
        {
            await ResetControlAsync();
            await PostgresReplicationCleanup.DropAsync(
                pg.ConnectionString, new WallabyNames(suffix, slot, publication));
        }
    }

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<bool> SlotExistsAsync(string slot)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1");
        cmd.Parameters.AddWithValue(slot);
        return (long)(await cmd.ExecuteScalarAsync())! > 0;
    }

    private async Task ResetControlAsync()
    {
        try
        {
            await using var cmd = pg.DataSource.CreateCommand("DELETE FROM wallaby.control");
            await cmd.ExecuteNonQueryAsync();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
        }
    }

    private static async Task PollUntilAsync(Func<Task<bool>> condition, string waitingFor)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!await condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException($"Timed out waiting for {waitingFor}.");
            }
            await Task.Delay(100);
        }
    }
}
