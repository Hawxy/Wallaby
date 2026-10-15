using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Aspire.Hosting.Internal;
using Wallaby.Client;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Aspire.Hosting.Tests.Integration;

/// <summary>
/// The monitor's poll, health grading and command callbacks against a real Wallaby host; the Aspire
/// orchestration around them (dependency waits, snapshot publishing) is covered by the sample AppHost test.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class WallabyMonitorTests(TestModelPostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    [Test]
    public async Task The_monitor_follows_a_host_from_provisioning_to_caught_up()
    {
        await ResetWallabyStateAsync();
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var client = new WallabyControlClient(pg.ConnectionString);
        var categoryId = await Db.AddCategoryAsync();
        await Db.AddProductAsync(categoryId, $"seed_{names.Suffix}");

        var before = await WallabyMonitor.PollAsync(client, previous: null, CancellationToken.None);
        WallabyObservationMapper.Health(before).Description.ShouldNotBeNull().ShouldContain("Waiting for a Wallaby host");

        await using var node = await StartHostAsync(names, new CaptureSink());

        var current = await PollUntilHealthyAsync(client);
        var properties = WallabyObservationMapper.Properties(current).ToDictionary(p => p.DisplayName!, p => p.Value);
        properties["Suspension"].ShouldBe("Running");
        properties[$"Slot {names.Slot}"].ShouldNotBeNull().ToString()!.ShouldStartWith("streaming");
        properties["Backfill public.products"].ShouldNotBeNull().ToString()!.ShouldStartWith("completed");
    }

    [Test]
    public async Task Dashboard_commands_drive_the_running_host()
    {
        await ResetWallabyStateAsync();
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var client = new WallabyControlClient(pg.ConnectionString);
        var categoryId = await Db.AddCategoryAsync();
        var productId = await Db.AddProductAsync(categoryId, $"cmd_{names.Suffix}");

        var capture = new CaptureSink();
        try
        {
            await using var node = await StartHostAsync(names, capture);
            await PollUntilHealthyAsync(client);

            var resource = CreateResource();
            resource.Monitor.Client = client;

            // Backfill with purge: the sink is purged, then the product is re-snapshotted.
            capture.Clear();
            var backfill = await ExecuteAsync(resource, WallabyCommands.Backfill,
                ("table", "public.products"), ("purge", "true"));
            backfill.Success.ShouldBeTrue(backfill.Message);
            await capture.WaitForDocumentsAsync([productId.ToString()]);
            capture.Purges.ShouldNotBeEmpty();
            await PollUntilHealthyAsync(client);

            // Suspend drops the slot; the monitor grades it Degraded.
            var suspend = await ExecuteAsync(resource, WallabyCommands.Suspend, ("reason", "dashboard test"));
            suspend.Success.ShouldBeTrue(suspend.Message);
            var suspended = await WallabyMonitor.PollAsync(client, previous: null, CancellationToken.None);
            suspended.Control.State.ShouldBe(WallabySuspensionState.Suspended);
            suspended.Control.Reason.ShouldBe("dashboard test");
            WallabyObservationMapper.Health(suspended).Status.ShouldBe(HealthStatus.Degraded);

            // Resume recreates the slot and re-backfills, after which the host is caught up again.
            var resume = await ExecuteAsync(resource, WallabyCommands.Resume, ("purge", "false"));
            resume.Success.ShouldBeTrue(resume.Message);
            await PollUntilHealthyAsync(client);
        }
        finally
        {
            await ExecAsync("DELETE FROM wallaby.control");
        }
    }

    [Test]
    public async Task Commands_against_a_database_no_host_has_run_on_explain_what_to_do()
    {
        var virgin = new NpgsqlConnectionStringBuilder(pg.ConnectionString) { Database = $"virgin_{Guid.NewGuid():N}" };
        await ExecAsync($"CREATE DATABASE {virgin.Database}");
        await using var client = new WallabyControlClient(virgin.ConnectionString);

        var resource = CreateResource();
        resource.Monitor.Client = client;

        var result = await ExecuteAsync(resource, WallabyCommands.Backfill, ("table", "public.products"), ("purge", "false"));

        result.Success.ShouldBeFalse();
        result.Message.ShouldNotBeNull().ShouldContain("No current Wallaby host has run against this database");
    }

    [Test]
    public async Task Commands_before_the_monitor_connects_fail_cleanly()
    {
        var result = await ExecuteAsync(CreateResource(), WallabyCommands.Restore);

        result.Success.ShouldBeFalse();
        result.Message.ShouldNotBeNull().ShouldContain("isn't connected");
    }

    private async Task<WallabyTestNode> StartHostAsync(ReplicationScope names, CaptureSink capture)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc => cdc
            .UseEntityFrameworkCore<AppDbContext>()
            .UseConnectionString(pg.ConnectionString)
            .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
            .WithMappings(sink => sink
                .Map<Product>()
                .ToDestination("products")
                .UsingTransform(TestTransforms.ProductNames)));
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.Advanced.ControlPollInterval = TimeSpan.FromMilliseconds(500);
        });
        services.ReplaceWallabySink("capture", capture);
        return await WallabyTestNode.StartAsync(services);
    }

    private static async Task<WallabyObservation> PollUntilHealthyAsync(WallabyControlClient client)
    {
        WallabyObservation? current = null;
        try
        {
            await Polling.UntilAsync(async () =>
            {
                current = await WallabyMonitor.PollAsync(client, current, CancellationToken.None);
                return WallabyObservationMapper.Health(current).Status == HealthStatus.Healthy;
            }, TimeSpan.FromSeconds(90));
        }
        catch (TimeoutException)
        {
            throw new TimeoutException($"Never caught up; last health: {WallabyObservationMapper.Health(current).Description}");
        }
        return current!;
    }

    private static WallabyResource CreateResource()
    {
        var app = DistributedApplication.CreateBuilder();
        var db = app.AddConnectionString("app");
        app.AddExecutable("worker", "dotnet", ".").WithWallaby(db);
        return app.Resources.OfType<WallabyResource>().Single();
    }

    private static Task<ExecuteCommandResult> ExecuteAsync(
        WallabyResource resource, string command, params (string Name, string Value)[] arguments)
    {
        var annotation = resource.Annotations.OfType<ResourceCommandAnnotation>().Single(c => c.Name == command);
        var inputs = annotation.Arguments
            .Select(declared => new InteractionInput
            {
                Name = declared.Name,
                InputType = declared.InputType,
                Value = arguments.SingleOrDefault(a => a.Name == declared.Name).Value ?? declared.Value,
            })
            .ToList();
        return annotation.ExecuteCommand(new ExecuteCommandContext
        {
            Services = new ServiceCollection().BuildServiceProvider(),
            ResourceName = resource.Name,
            CancellationToken = CancellationToken.None,
            Logger = NullLogger.Instance,
            Arguments = new InteractionInputCollection(inputs),
        });
    }

    private async Task ResetWallabyStateAsync()
    {
        await WallabyStateSchema.EnsureAsync(pg.DataSource);
        await ExecAsync("DELETE FROM wallaby.slot_registry; DELETE FROM wallaby.backfill_state; DELETE FROM wallaby.control;");
    }

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }
}
