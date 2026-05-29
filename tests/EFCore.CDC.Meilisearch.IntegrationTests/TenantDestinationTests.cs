using EFCore.CDC.Meilisearch;
using EFCore.CDC.Testing;
using EFCore.CDC.TestModel;
using TUnit.Core.Interfaces;

namespace EFCore.CDC.Meilisearch.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class TenantDestinationTests(PostgresFixture pg, MeilisearchFixture meili)
{
    [Test]
    public async Task Documents_route_to_a_per_tenant_index_including_deletes()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var prefix = harness.Names.Named("products"); // unique per test run
        harness.AddSink(new MeilisearchSink("meili", new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey }))
            .Project<Product>("meili", destination: null,
                document: p => new Dictionary<string, object?> { ["name"] = p.Name },
                scopeKey: p => p.TenantId,
                scopedDestination: key => $"{prefix}_{key}");

        await harness.SelfConfigureAsync();
        // Scoped destination needs the tenant key on deletes too.
        await harness.Db.SetReplicaIdentityFullAsync("products");

        var categoryId = await harness.Db.AddCategoryAsync();
        var t1 = await harness.Db.AddProductAsync(categoryId, "t1a", tenantId: 1);
        var t2 = await harness.Db.AddProductAsync(categoryId, "t2a", tenantId: 2);

        var probe = new MeiliProbe(meili);
        await harness.RunUntilAsync(async () =>
            await probe.NameAsync($"{prefix}_1", t1) == "t1a" && await probe.NameAsync($"{prefix}_2", t2) == "t2a");

        // Each document landed only in its own tenant index.
        await Assert.That(await probe.NameAsync($"{prefix}_1", t1)).IsEqualTo("t1a");
        await Assert.That(await probe.NameAsync($"{prefix}_2", t2)).IsEqualTo("t2a");
        await Assert.That(await probe.NameAsync($"{prefix}_2", t1)).IsNull(); // not cross-indexed

        // Delete tenant 1's product -> removed from the tenant-1 index, tenant-2 untouched.
        await harness.Db.DeleteProductAsync(t1);
        await harness.RunUntilAsync(async () => await probe.NameAsync($"{prefix}_1", t1) is null);

        await Assert.That(await probe.NameAsync($"{prefix}_1", t1)).IsNull();
        await Assert.That(await probe.NameAsync($"{prefix}_2", t2)).IsEqualTo("t2a");
    }
}
