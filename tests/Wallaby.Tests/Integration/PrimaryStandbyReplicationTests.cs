using System.Text;
using DotNet.Testcontainers.Builders;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;
using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Model;

namespace Wallaby.Tests.Integration;

/// <summary>
/// The replication path against a live primary/standby pair: a multi-host connection string listing
/// the standby first must resolve to the primary and stream from it (Npgsql rejects a multi-host
/// replication connection outright, so the resolver's probing is the only route), which only a real
/// pair can prove.
/// </summary>
[NotInParallel]
public class PrimaryStandbyReplicationTests
{
    [Test]
    public async Task A_multi_host_connection_string_streams_from_the_primary()
    {
        await using var network = new NetworkBuilder().Build();

        // The stock image's pg_hba has no "replication" entry, which pg_basebackup needs.
        var allowReplication = "#!/bin/bash\necho 'host replication all all trust' >> \"$PGDATA/pg_hba.conf\"\n";
        await using var primary = new PostgreSqlBuilder("postgres:17")
            .WithNetwork(network)
            .WithNetworkAliases("pg-primary")
            .WithCommand("-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10")
            .WithResourceMapping(Encoding.UTF8.GetBytes(allowReplication), "/docker-entrypoint-initdb.d/zz_replication.sh")
            .Build();
        await primary.StartAsync();

        // A hot standby cloned from the primary; -R writes standby.signal + primary_conninfo, and the
        // basebackup retries until the primary accepts replication connections.
        var standbyScript =
            "chown -R postgres:postgres /var/lib/postgresql && chmod 700 \"$PGDATA\" 2>/dev/null || true; " +
            "until gosu postgres pg_basebackup -h pg-primary -U postgres -D \"$PGDATA\" -R; " +
            "do rm -rf \"$PGDATA\"; sleep 1; done; " +
            "exec gosu postgres postgres";
        await using var standby = new ContainerBuilder()
            .WithImage("postgres:17")
            .WithNetwork(network)
            .WithNetworkAliases("pg-standby")
            .WithPortBinding(5432, assignRandomHostPort: true)
            .WithEntrypoint("bash", "-c")
            .WithCommand(standbyScript)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted("pg_isready", "-h", "127.0.0.1", "-U", "postgres"))
            .Build();
        await standby.StartAsync();

        var primaryBuilder = new NpgsqlConnectionStringBuilder(primary.GetConnectionString());
        var standbyHostSpec = $"{standby.Hostname}:{standby.GetMappedPublicPort(5432)}";
        var primaryHostSpec = $"{primaryBuilder.Host}:{primaryBuilder.Port}";

        // Prove the first-listed host really is a standby, so host selection is what's under test.
        var standbyBuilder = new NpgsqlConnectionStringBuilder(primary.GetConnectionString())
        {
            Host = standby.Hostname,
            Port = standby.GetMappedPublicPort(5432),
        };
        await using (var standbyDataSource = NpgsqlDataSource.Create(standbyBuilder.ConnectionString))
        {
            await WaitUntilInRecoveryAsync(standbyDataSource);
        }

        await using var dataSource = NpgsqlDataSource.Create(primary.GetConnectionString());
        await using (var create = dataSource.CreateCommand("CREATE TABLE probe (id int PRIMARY KEY)"))
        {
            await create.ExecuteNonQueryAsync();
        }

        var table = new CapturedTable
        {
            EntityClrType = typeof(object), Schema = "public", TableName = "probe",
            Columns = [], PrimaryKey = [],
        };
        var configurator = new PostgresSelfConfigurator(
            dataSource,
            new SelfConfigOptions { SlotName = "b3_probe_slot", PublicationName = "b3_probe_pub" },
            NullLogger.Instance);
        await configurator.EnsureConfiguredAsync(new WallabyModel([table]), CancellationToken.None);

        await using (var insert = dataSource.CreateCommand("INSERT INTO probe (id) VALUES (42)"))
        {
            await insert.ExecuteNonQueryAsync();
        }

        // Standby listed first: only probing can route past it to the host that holds the slot.
        var multiHostBuilder = new NpgsqlConnectionStringBuilder(primary.GetConnectionString())
        {
            Host = $"{standbyHostSpec},{primaryHostSpec}",
        };
        multiHostBuilder.Remove("Port");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var resolved = await ReplicationPrimaryResolver.ResolveAsync(multiHostBuilder.ConnectionString, cts.Token);
        var resolvedBuilder = new NpgsqlConnectionStringBuilder(resolved);
        $"{resolvedBuilder.Host}:{resolvedBuilder.Port}".ShouldBe(primaryHostSpec);
        resolvedBuilder.Password.ShouldBe(primaryBuilder.Password); // credentials survive the rebuild

        await using var spill = new PostgresUnloggedTableSpill(dataSource, "b3_probe_slot");
        await using var stream = new LogicalReplicationStream(resolved, "b3_probe_slot", "b3_probe_pub", spill);

        RawChange? received = null;
        await foreach (var txn in stream.ReadAsync(cts.Token))
        {
            received = txn.Changes.FirstOrDefault(c => c.TableName == "probe");
            if (received is not null)
            {
                break;
            }
        }

        received.ShouldNotBeNull();
        received!.Action.ShouldBe(ChangeAction.Insert);
        received.NewValues.Single(c => c.ColumnName == "id").Value.ShouldBe(42);
    }

    private static async Task WaitUntilInRecoveryAsync(NpgsqlDataSource standby)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (true)
        {
            try
            {
                await using var cmd = standby.CreateCommand("SELECT pg_is_in_recovery()");
                if (await cmd.ExecuteScalarAsync() is true)
                {
                    return;
                }
                throw new InvalidOperationException("The standby container is not in recovery.");
            }
            catch (Exception) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(500);
            }
        }
    }
}
