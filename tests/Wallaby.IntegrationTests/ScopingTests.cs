using System.Collections.Concurrent;
using EFCore.CDC.TestInfrastructure;
using EFCore.CDC.TestModel;
using Wallaby.Abstractions;

namespace EFCore.CDC.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class ScopingTests(PostgresFixture pg)
{
    [Test]
    public async Task Enrichment_context_is_built_per_scope_key_and_transform_batches_are_single_tenant()
    {
        var contextScopeKeys = new ConcurrentBag<object?>();
        var perInvocationTenantCounts = new ConcurrentBag<int>();

        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness
            .UseScopedContext(scopeKey =>
            {
                contextScopeKeys.Add(scopeKey);                       // a context is built per tenant
                return new AppDbContext(TestModelFactory.CreateOptions(pg.ConnectionString));
            })
            .Map<Product>("capture", destination: null, (_, changes, _) =>
            {
                // The engine must hand us a single-tenant batch.
                var tenants = changes.Select(c => ((Product)c.Entity!).TenantId).Distinct().ToList();
                perInvocationTenantCounts.Add(tenants.Count);

                var docs = changes.ToDictionary(
                    c => c.Key,
                    c => (object?)new Dictionary<string, object?> { ["tenant"] = ((Product)c.Entity!).TenantId });
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, object?>>(docs);
            }, scopeKey: p => p.TenantId);

        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        // Four products across two tenants, committed in ONE transaction => one batch spanning tenants.
        await harness.Db.AddProductsAsync(categoryId,
            [("t1a", 1), ("t2a", 2), ("t1b", 1), ("t2b", 2)]);

        await harness.RunUntilAsync(() => capture.For("products").Count() >= 4);

        // Every transform invocation saw exactly one tenant (the engine sub-grouped the batch).
        await Assert.That(perInvocationTenantCounts).IsNotEmpty();
        await Assert.That(perInvocationTenantCounts.All(count => count == 1)).IsTrue();

        // A scoped enrichment context was built for both tenants.
        var distinctKeys = contextScopeKeys.Select(k => (int)k!).Distinct().OrderBy(k => k).ToList();
        await Assert.That(distinctKeys).IsEquivalentTo(new[] { 1, 2 });
    }
}
