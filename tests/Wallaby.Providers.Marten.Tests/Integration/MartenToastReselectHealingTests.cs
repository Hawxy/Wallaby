using Marten;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.Marten;

namespace Wallaby.Providers.Marten.Tests.Integration;

/// <summary>
/// Reselect healing for Marten documents: under REPLICA IDENTITY DEFAULT an un-delete
/// (<c>UPDATE ... SET mt_deleted = false</c>) doesn't assign <c>data</c>, so a TOASTed body is not on
/// the wire; the pipeline re-reads the row and delivers the restored document instead of halting.
/// </summary>
[NotInParallel]
[ClassDataSource<MartenStoreFixture>(Shared = SharedType.PerTestSession)]
public class MartenToastReselectHealingTests(MartenStoreFixture pg)
{
    [Test]
    public async Task An_undelete_with_an_unavailable_body_heals_by_reselect()
    {
        await using var harness = WallabyTestHarness.ForMartenStore(pg.Store, pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<SoftWidget>("capture", "softwidgets", w => new WallabyDocument { ["name"] = w.Name });
        using var reselected = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.changes.reselected");

        // The fixture leaves soft-delete tables on FULL; this test needs the omitted-body wire shape.
        await SetReplicaIdentityAsync("mt_doc_softwidget", "DEFAULT");
        try
        {
            await harness.SelfConfigureAsync();

            var id = Guid.NewGuid();
            var name = $"lazarus_{Guid.NewGuid():N}";
            // Base64 of seeded random bytes: incompressible, so the body is stored out-of-line and an
            // update that doesn't assign data omits it from the wire.
            var payloadBytes = new byte[16_000];
            new Random(42).NextBytes(payloadBytes);
            await using (var session = pg.Store.LightweightSession())
            {
                session.Store(new SoftWidget { Id = id, Name = name, Payload = Convert.ToBase64String(payloadBytes) });
                await session.SaveChangesAsync();
            }

            await harness.StartAsync();
            await harness.WaitUntilAsync(() => capture.For("mt_doc_softwidget").Any(r => !r.IsDeletion));

            await using (var session = pg.Store.LightweightSession())
            {
                session.Delete<SoftWidget>(id);
                await session.SaveChangesAsync();
            }
            await harness.WaitUntilAsync(() => capture.For("mt_doc_softwidget").Any(r => r.IsDeletion));

            await using (var session = pg.Store.LightweightSession())
            {
                session.UndoDeleteWhere<SoftWidget>(w => w.Id == id);
                await session.SaveChangesAsync();
            }
            await harness.WaitUntilAsync(() => capture.For("mt_doc_softwidget").Count(r => !r.IsDeletion) >= 2);
            await harness.StopAsync();

            // The restored document rehydrated from the reselected row, not from the wire.
            capture.For("mt_doc_softwidget").Last(r => !r.IsDeletion)
                .Document?.GetValueOrDefault("name").ShouldBe(name);
            reselected.GetMeasurementSnapshot()
                .Where(m => Equals(m.Tags.GetValueOrDefault("wallaby.reselect.outcome"), "healed"))
                .Sum(m => m.Value).ShouldBe(1);
        }
        finally
        {
            // Later tests expect the fixture's FULL identity on soft-delete tables.
            await SetReplicaIdentityAsync("mt_doc_softwidget", "FULL");
        }
    }

    private async Task SetReplicaIdentityAsync(string table, string identity)
    {
        await using var connection = new NpgsqlConnection(pg.ConnectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"ALTER TABLE {MartenTestStore.Schema}.{table} REPLICA IDENTITY {identity}", connection);
        await cmd.ExecuteNonQueryAsync();
    }
}
