using System.Collections.Concurrent;
using Marten;
using Wallaby.Abstractions;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.Marten;

namespace Wallaby.Marten.IntegrationTests;

/// <summary>
/// Conjoined tenancy: the captured key is [tenant_id, id] so equal ids across tenants stay distinct,
/// transform batches are sub-grouped per tenant with tenant-scoped query sessions, scoped destinations
/// route per tenant, and a delete resolves its tenant from the key columns.
/// </summary>
[NotInParallel]
[ClassDataSource<MartenStoreFixture>(Shared = SharedType.PerTestSession)]
public class MartenTenancyTests(MartenStoreFixture pg)
{
    [Test]
    public async Task Equal_ids_across_tenants_route_to_tenant_scoped_destinations_and_sessions()
    {
        var sessionTenants = new ConcurrentBag<string>();
        var perInvocationTenantCounts = new ConcurrentBag<int>();

        await using var harness = WallabyTestHarness.ForMartenStore(pg.Store, pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness
            .UseTenantSessions(pg.Store)
            .Map<TenantWidget>("capture", destination: null, (session, changes, _) =>
            {
                sessionTenants.Add(session.TenantId); // the leased session is tenant-scoped

                // The engine must hand us a single-tenant batch.
                var tenants = changes.Select(c => (string)c.Record["TenantId"]!).Distinct().ToList();
                perInvocationTenantCounts.Add(tenants.Count);

                var documents = changes.ToDictionary(
                    c => c.Key,
                    c => (WallabyDocument?)new WallabyDocument { ["name"] = ((TenantWidget)c.Entity!).Name });
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
            },
            scopeKey: WallabyTestHarnessMartenExtensions.TenantScopeKey,
            scopedDestination: tenant => $"idx-{tenant}");

        await harness.SelfConfigureAsync();

        // The same document id in two tenants, committed in ONE transaction => one batch spanning tenants.
        await using (var session = pg.Store.LightweightSession("kanga"))
        {
            session.Store(new TenantWidget { Id = "doc-1", Name = "kanga-doc" });
            await session.SaveChangesAsync();
        }
        await using (var session = pg.Store.LightweightSession("roo"))
        {
            session.Store(new TenantWidget { Id = "doc-1", Name = "roo-doc" });
            await session.SaveChangesAsync();
        }

        await harness.StartAsync();
        await harness.WaitUntilAsync(() => capture.Records.Count(r => !r.IsDeletion) >= 2);

        // Distinct sink documents per tenant, routed to per-tenant destinations.
        var upserts = capture.Records.Where(r => !r.IsDeletion).ToList();
        upserts.Single(r => r.Destination == "idx-kanga").Document!["name"].ShouldBe("kanga-doc");
        upserts.Single(r => r.Destination == "idx-roo").Document!["name"].ShouldBe("roo-doc");

        perInvocationTenantCounts.ShouldAllBe(count => count == 1);
        sessionTenants.ShouldContain("kanga");
        sessionTenants.ShouldContain("roo");

        // A delete in one tenant resolves its tenant from the key columns and removes only that copy.
        await using (var session = pg.Store.LightweightSession("kanga"))
        {
            session.Delete<TenantWidget>("doc-1");
            await session.SaveChangesAsync();
        }
        await harness.WaitUntilAsync(() => capture.Records.Any(r => r.IsDeletion));
        await harness.StopAsync();

        var deletion = capture.Records.Single(r => r.IsDeletion);
        deletion.Destination.ShouldBe("idx-kanga");
    }
}
