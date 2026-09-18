using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.Tables;

namespace Wallaby.Providers.Tables.Tests.Integration;

/// <summary>
/// End-to-end capture of plain tables: inserts, updates (with the old-value diff under REPLICA IDENTITY
/// FULL) and deletes, positional records with enum and timestamp coercion, composite keys through
/// backfill and live changes, column lists from <c>Consumes</c>, and TOAST reselect healing.
/// </summary>
[NotInParallel]
[ClassDataSource<TablesFixture>(Shared = SharedType.PerTestSession)]
public class TablesPipelineTests(TablesFixture pg)
{
    private static string? NameOf(SinkRecord r) => r.Document?.GetValueOrDefault("name") as string;

    private async Task ExecAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name, value);
        }
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<int> InsertOrderAsync(string customer, decimal total, string status = "Paid", string? notes = null)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            $"INSERT INTO {TablesFixture.Schema}.orders (customer_ref, total, status, notes) VALUES (@c, @t, @s, @n) RETURNING id");
        cmd.Parameters.AddWithValue("c", customer);
        cmd.Parameters.AddWithValue("t", total);
        cmd.Parameters.AddWithValue("s", status);
        cmd.Parameters.AddWithValue("n", (object?)notes ?? DBNull.Value);
        return (int)(await cmd.ExecuteScalarAsync())!;
    }

    [Test]
    public async Task Insert_update_and_delete_flow_to_the_sink_with_the_old_values()
    {
        await using var harness = WallabyTestHarness.ForTestTables(pg);
        var capture = harness.AddCaptureSink();
        harness.Map<Gizmo>("capture", "gizmos", (_, changes, _) =>
            Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(changes.ToDictionary(
                c => c.Key,
                c => (WallabyDocument?)new WallabyDocument
                {
                    ["name"] = c.Entity!.DisplayName,
                    ["price"] = c.Entity.UnitPrice,
                    ["previous_name"] = c.Changes?.GetValueOrDefault("DisplayName"),
                })));
        await harness.SelfConfigureAsync();
        await harness.StartAsync();

        var id = Guid.NewGuid();
        await ExecAsync($"INSERT INTO {TablesFixture.Schema}.gizmo VALUES (@id, 'kanga', 9.5)", ("id", id));
        await harness.WaitUntilAsync(() => capture.For("gizmo").Any(r => NameOf(r) == "kanga"));

        await ExecAsync($"UPDATE {TablesFixture.Schema}.gizmo SET display_name = 'roo' WHERE id = @id", ("id", id));
        await harness.WaitUntilAsync(() => capture.For("gizmo").Any(r => NameOf(r) == "roo"));

        await ExecAsync($"DELETE FROM {TablesFixture.Schema}.gizmo WHERE id = @id", ("id", id));
        await harness.WaitUntilAsync(() => capture.For("gizmo").Any(r => r.IsDeletion));
        await harness.StopAsync();

        var update = capture.For("gizmo").Single(r => NameOf(r) == "roo");
        update.Document!["price"].ShouldBe(9.5m);
        // REPLICA IDENTITY FULL carries the whole old tuple, so the diff names the renamed column.
        update.Document["previous_name"].ShouldBe("kanga");
        capture.LatestByDocumentId()[id.ToString()].IsDeletion.ShouldBeTrue();
    }

    [Test]
    public async Task Positional_records_materialize_with_enum_and_timestamp_coercion()
    {
        await using var harness = WallabyTestHarness.ForTestTables(pg);
        var capture = harness.AddCaptureSink();
        harness.Project<Order>("capture", "orders", o => new WallabyDocument
        {
            ["name"] = o.CustomerRef,
            ["status"] = o.Status,
            ["total"] = o.Total,
            ["created"] = o.CreatedAt,
        });
        await harness.SelfConfigureAsync();
        await harness.StartAsync();

        var customer = $"acme_{Guid.NewGuid():N}";
        var id = await InsertOrderAsync(customer, 12.5m, "Shipped");
        await harness.WaitUntilAsync(() => capture.For("orders").Any(r => NameOf(r) == customer));
        await harness.StopAsync();

        var record = capture.For("orders").Single(r => NameOf(r) == customer);
        record.DocumentId.ShouldBe(id.ToString());
        record.Document!["status"].ShouldBe(OrderStatus.Shipped);
        record.Document["total"].ShouldBe(12.5m);
        record.Document["created"].ShouldBeOfType<DateTimeOffset>().ShouldBeGreaterThan(DateTimeOffset.UtcNow.AddMinutes(-5));
    }

    [Test]
    public async Task Composite_keys_backfill_and_stream()
    {
        // Seeded before the slot exists: only a backfill can deliver these.
        var orderId = Random.Shared.Next(100_000, 1_000_000);
        await ExecAsync($"INSERT INTO {TablesFixture.Schema}.order_lines VALUES (@o, 1, 'sku-a', 1), (@o, 2, 'sku-b', 2)", ("o", orderId));

        await using var harness = WallabyTestHarness.ForTestTables(pg);
        harness.ChunkSize = 1;
        var capture = harness.AddCaptureSink();
        harness.Project<OrderLine>("capture", "lines",
            l => new WallabyDocument { ["order"] = l.OrderId, ["line"] = l.LineNo, ["sku"] = l.Sku }, backfill: true);
        await harness.SelfConfigureAsync();
        await harness.StartAsync();
        await harness.RunBackfillAsync();
        await harness.WaitUntilAsync(() => capture.For("order_lines").Count(r => r.Metadata.IsBackfill && Equals(r.Document?["order"], orderId)) == 2);

        await ExecAsync($"INSERT INTO {TablesFixture.Schema}.order_lines VALUES (@o, 3, 'sku-c', 3)", ("o", orderId));
        await harness.WaitUntilAsync(() => capture.For("order_lines").Any(r => !r.Metadata.IsBackfill && Equals(r.Document?["sku"], "sku-c")));
        await harness.StopAsync();

        var mine = capture.For("order_lines").Where(r => Equals(r.Document?["order"], orderId)).ToList();
        // Three distinct document ids, keyed on (order_id, line_no) in that order.
        mine.Select(r => r.DocumentId).Distinct().Count().ShouldBe(3);
        mine.Select(r => r.DocumentId).ShouldAllBe(id => id.StartsWith(orderId.ToString()));
        mine.Where(r => r.Metadata.IsBackfill).Select(r => r.Document!["line"]).ShouldBe([1, 2]);
    }

    [Test]
    public async Task Consumes_publishes_a_column_list_and_leaves_the_rest_at_defaults()
    {
        await using var harness = WallabyTestHarness.ForTestTables(pg)
            .Consumes<Order>(nameof(Order.CustomerRef));
        var capture = harness.AddCaptureSink();
        harness.Project<Order>("capture", "orders", o => new WallabyDocument { ["name"] = o.CustomerRef, ["total"] = o.Total, ["status"] = o.Status });
        await harness.SelfConfigureAsync();

        await using (var cmd = pg.DataSource.CreateCommand(
            "SELECT array_to_string(attnames, ',') FROM pg_publication_tables WHERE pubname = @p AND tablename = 'orders'"))
        {
            cmd.Parameters.AddWithValue("p", harness.Names.Publication);
            (await cmd.ExecuteScalarAsync()).ShouldBe("id,customer_ref");
        }

        await harness.StartAsync();
        var customer = $"narrow_{Guid.NewGuid():N}";
        await InsertOrderAsync(customer, 99m);
        await harness.WaitUntilAsync(() => capture.For("orders").Any(r => NameOf(r) == customer));
        await harness.StopAsync();

        var document = capture.For("orders").Single(r => NameOf(r) == customer).Document!;
        document["total"].ShouldBe(0m);
        document["status"].ShouldBe(OrderStatus.Pending);
    }

    [Test]
    public async Task An_unchanged_toasted_value_heals_by_reselect()
    {
        await using var harness = WallabyTestHarness.ForTestTables(pg);
        var capture = harness.AddCaptureSink();
        harness.Project<Order>("capture", "orders", o => new WallabyDocument { ["name"] = o.CustomerRef, ["notes"] = o.Notes });
        using var reselected = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.changes.reselected");
        await harness.SelfConfigureAsync();
        await harness.StartAsync();

        // Incompressible notes are stored out-of-line; an update that leaves them untouched omits them
        // from the new tuple under the table's default replica identity.
        var payload = new byte[16_000];
        new Random(42).NextBytes(payload);
        var notes = Convert.ToBase64String(payload);
        var customer = $"toast_{Guid.NewGuid():N}";
        var id = await InsertOrderAsync(customer, 1m, notes: notes);
        await harness.WaitUntilAsync(() => capture.For("orders").Any(r => NameOf(r) == customer));

        await ExecAsync($"UPDATE {TablesFixture.Schema}.orders SET customer_ref = @c WHERE id = @id", ("c", customer + "_renamed"), ("id", id));
        await harness.WaitUntilAsync(() => capture.For("orders").Any(r => NameOf(r) == customer + "_renamed"));
        await harness.StopAsync();

        capture.For("orders").Single(r => NameOf(r) == customer + "_renamed").Document!["notes"].ShouldBe(notes);
        reselected.GetMeasurementSnapshot()
            .Where(m => Equals(m.Tags.GetValueOrDefault("wallaby.reselect.outcome"), "healed"))
            .Sum(m => m.Value).ShouldBe(1);
    }
}
