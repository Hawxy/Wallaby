using System.Net;
using System.Net.Http.Headers;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;
using Wallaby.Aspire.Hosting.Internal;
using Wallaby.TestInfrastructure;

namespace Wallaby.Aspire.Hosting.Tests.Integration;

/// <summary>
/// The sample AppHost end to end under Aspire's orchestrator: logical replication on Aspire's Postgres
/// container, the worker streaming into Meilisearch, and the Wallaby resource's health and commands.
/// </summary>
[NotInParallel]
public class SampleAppHostTests
{
    private const string Wallaby = "worker-wallaby";

    [Test]
    public async Task The_sample_streams_to_meilisearch_and_the_dashboard_resource_follows_suspension()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var ct = timeout.Token;

        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.Wallaby_Aspire_Sample_AppHost>(ct);
        await using var app = await appHost.BuildAsync(ct);
        await app.StartAsync(ct);
        var notifications = app.Services.GetRequiredService<ResourceNotificationService>();
        var commands = app.Services.GetRequiredService<ResourceCommandService>();

        // Healthy means the slot streams on Aspire's Postgres, which needs WithLogicalReplication.
        await notifications.WaitForResourceHealthyAsync(Wallaby, ct);

        var productId = await InsertProductAsync((await app.GetConnectionStringAsync("app", ct))!, ct);
        using var meilisearch = await CreateMeilisearchClientAsync(app, ct);
        await Polling.UntilAsync(async () =>
            (await meilisearch.GetAsync($"indexes/products/documents/{productId}", ct)).StatusCode == HttpStatusCode.OK,
            TimeSpan.FromSeconds(60));

        // Aspire re-checks a healthy resource only every 30 seconds; the monitor's own health report
        // must surface the suspension well before that.
        var suspend = await commands.ExecuteCommandAsync(Wallaby, WallabyCommands.Suspend, ct);
        suspend.Success.ShouldBeTrue(suspend.Message);
        using (var quickly = CancellationTokenSource.CreateLinkedTokenSource(ct))
        {
            quickly.CancelAfter(TimeSpan.FromSeconds(10));
            await notifications.WaitForResourceAsync(Wallaby,
                e => e.Snapshot.HealthStatus == HealthStatus.Degraded, quickly.Token);
        }

        var resume = await commands.ExecuteCommandAsync(Wallaby, WallabyCommands.Resume, ct);
        resume.Success.ShouldBeTrue(resume.Message);
        await notifications.WaitForResourceAsync(Wallaby,
            e => e.Snapshot.HealthStatus == HealthStatus.Healthy
                 && e.Snapshot.Properties.Any(p => p.Name == "wallaby.suspension" && Equals(p.Value, "Running")),
            ct);
    }

    private static async Task<int> InsertProductAsync(string connectionString, CancellationToken ct)
    {
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await using var cmd = dataSource.CreateCommand(
            """INSERT INTO products ("Name", "Price", "Category") VALUES ('Aspire e2e', 9.99, 'tests') RETURNING "Id" """);
        return (int)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task<HttpClient> CreateMeilisearchClientAsync(DistributedApplication app, CancellationToken ct)
    {
        var model = app.Services.GetRequiredService<DistributedApplicationModel>();
        var masterKey = await model.Resources.OfType<MeilisearchResource>().Single().MasterKeyParameter.GetValueAsync(ct);
        var client = new HttpClient { BaseAddress = app.GetEndpoint("meilisearch", "http") };
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", masterKey);
        return client;
    }
}
