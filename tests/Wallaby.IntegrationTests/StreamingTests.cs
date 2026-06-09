using EFCore.CDC.TestModel;
using Npgsql;
using Testcontainers.PostgreSql;
using Wallaby.TestInfrastructure;

namespace Wallaby.IntegrationTests;

/// <summary>
/// Exercises pgoutput <b>v2 streaming</b>: a single transaction larger than the server's
/// <c>logical_decoding_work_mem</c> is streamed before commit (StreamStart/Stop/Commit), driving the
/// <c>TransactionAssembler</c>'s per-xid streaming path rather than the normal Begin..Commit buffer. Uses a
/// dedicated container configured with a tiny <c>logical_decoding_work_mem</c> so a modest transaction streams.
/// </summary>
[NotInParallel]
public class StreamingTests
{
    [Test]
    public async Task Large_transaction_is_streamed_assembled_and_delivered()
    {
        await using var container = new PostgreSqlBuilder("postgres:17")
            .WithCommand(
                "-c", "wal_level=logical",
                "-c", "logical_decoding_work_mem=64kB",
                "-c", "max_replication_slots=20",
                "-c", "max_wal_senders=20")
            .Build();
        await container.StartAsync();
        var connectionString = container.GetConnectionString();

        await using (var ctx = new AppDbContext(TestModelFactory.CreateOptions(connectionString)))
        {
            await ctx.Database.EnsureCreatedAsync();
        }

        await using var harness = CdcTestHarness.ForTestModel(connectionString).Broadcast();
        var capture = harness.AddCaptureSink();
        await harness.SelfConfigureAsync();

        // One transaction with enough changes to exceed the 64kB reorder-buffer limit, so the server streams it.
        const int count = 2000;
        var categoryId = await harness.Db.AddCategoryAsync();
        var items = Enumerable.Range(0, count).Select(i => ($"p{i}", 0)).ToArray();
        await harness.Db.AddProductsAsync(categoryId, items);

        // All rows of the streamed transaction are delivered (none dropped by the streaming state machine).
        await harness.RunUntilAsync(
            () => capture.For("products").Count(r => r.Document is not null) >= count,
            timeout: TimeSpan.FromSeconds(90));

        var delivered = capture.For("products").Count(r => r.Document is not null);
        await Assert.That(delivered).IsEqualTo(count);

        // And the server actually streamed it (proves the v2 path engaged, not buffer-then-send at commit).
        await Polling.UntilAsync(async () => await StreamTxnsAsync(connectionString, harness.Names.Slot) > 0L,
            TimeSpan.FromSeconds(20));
    }

    private static async Task<long> StreamTxnsAsync(string connectionString, string slot)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT coalesce(stream_txns, 0) FROM pg_stat_replication_slots WHERE slot_name = @s", conn);
        cmd.Parameters.AddWithValue("s", slot);
        var result = await cmd.ExecuteScalarAsync();
        return result switch { long l => l, int i => i, _ => 0L };
    }
}
