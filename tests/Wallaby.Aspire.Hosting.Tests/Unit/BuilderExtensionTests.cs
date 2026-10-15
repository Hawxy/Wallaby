using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Wallaby.Aspire.Hosting.Internal;

namespace Wallaby.Aspire.Hosting.Tests.Unit;

public class BuilderExtensionTests
{
    [Test]
    public void WithWallaby_adds_a_child_resource_that_waits_for_the_database()
    {
        var app = DistributedApplication.CreateBuilder();
        var db = app.AddPostgres("pg").AddDatabase("app");
        var worker = app.AddExecutable("worker", "dotnet", ".");

        worker.WithWallaby(db, w => w.WithPollInterval(TimeSpan.FromMilliseconds(500)));

        var wallaby = app.Resources.OfType<WallabyResource>().ShouldHaveSingleItem();
        wallaby.Name.ShouldBe("worker-wallaby");
        wallaby.Host.ShouldBeSameAs(worker.Resource);
        wallaby.Database.ShouldBeSameAs(db.Resource);
        wallaby.PollInterval.ShouldBe(TimeSpan.FromMilliseconds(500));

        wallaby.Annotations.OfType<ResourceRelationshipAnnotation>()
            .ShouldContain(r => r.Type == "Parent" && r.Resource == worker.Resource);
        wallaby.Annotations.OfType<WaitAnnotation>().ShouldContain(w => w.Resource == db.Resource);
        wallaby.Annotations.OfType<HealthCheckAnnotation>().ShouldHaveSingleItem().Key.ShouldBe("worker-wallaby_wallaby");
        wallaby.Annotations.OfType<ManifestPublishingCallbackAnnotation>().ShouldNotBeEmpty();
        wallaby.Annotations.OfType<ResourceCommandAnnotation>().Select(c => c.Name).ShouldBe(
        [
            WallabyCommands.Backfill, WallabyCommands.BackfillAll, WallabyCommands.CancelBackfill,
            WallabyCommands.Suspend, WallabyCommands.Resume, WallabyCommands.Widen, WallabyCommands.Restore,
        ]);
    }

    [Test]
    public void WithWallaby_accepts_a_name_and_a_connection_string_resource()
    {
        var app = DistributedApplication.CreateBuilder();
        var db = app.AddConnectionString("existing");

        app.AddExecutable("worker", "dotnet", ".").WithWallaby(db, name: "cdc");

        app.Resources.OfType<WallabyResource>().ShouldHaveSingleItem().Name.ShouldBe("cdc");
    }

    [Test]
    public void WaitForWallaby_waits_on_the_hosts_wallaby_resource()
    {
        var app = DistributedApplication.CreateBuilder();
        var db = app.AddPostgres("pg").AddDatabase("app");
        var worker = app.AddExecutable("worker", "dotnet", ".").WithWallaby(db);
        var api = app.AddExecutable("api", "dotnet", ".");

        api.WaitForWallaby(worker);

        var wallaby = app.Resources.OfType<WallabyResource>().Single();
        api.Resource.Annotations.OfType<WaitAnnotation>().ShouldContain(w => w.Resource == wallaby);
    }

    [Test]
    public void WaitForWallaby_on_a_host_without_wallaby_throws()
    {
        var app = DistributedApplication.CreateBuilder();
        var worker = app.AddExecutable("worker", "dotnet", ".");
        var api = app.AddExecutable("api", "dotnet", ".");

        Should.Throw<InvalidOperationException>(() => api.WaitForWallaby(worker))
            .Message.ShouldContain("call WithWallaby on it");
    }

    [Test]
    public async Task WithLogicalReplication_passes_the_server_settings_as_postgres_arguments()
    {
        var app = DistributedApplication.CreateBuilder();
        var pg = app.AddPostgres("pg").WithLogicalReplication(maxReplicationSlots: 4);

        var args = new List<object>();
        foreach (var callback in pg.Resource.Annotations.OfType<CommandLineArgsCallbackAnnotation>())
        {
            await callback.Callback(new CommandLineArgsCallbackContext(args, pg.Resource));
        }

        args.ShouldBe(
            ["-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4"],
            ignoreOrder: false);
    }
}
