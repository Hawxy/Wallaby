using Npgsql;
using Wallaby.TestInfrastructure;

namespace Wallaby.Client.Tests.Integration;

/// <summary>
/// Remote backfill control against a bare database; the client persists requests and reads status by
/// table name; the host-side scheduling is covered by the EF Core end-to-end tests.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class BackfillClientTests(PostgresFixture pg)
{
    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    // The client performs no DDL, so these tests simulate a Wallaby host having run against the
    // database with the real bootstrapper (single source of truth for the wallaby DDL).
    private Task EnsureBackfillTableAsync() => WallabyStateSchema.EnsureAsync(pg.DataSource);

    [Test]
    public async Task Request_marks_the_table_requested_and_preserves_its_transform_version()
    {
        var table = $"public.orders_{Guid.NewGuid():N}";
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureBackfillTableAsync();
            await ExecAsync(
                $$"""
                  INSERT INTO wallaby.backfill_state (table_qualified, status, transform_version, cursor_json, rows_copied)
                  VALUES ('{{table}}', 'Completed', 'v2', '{"k":1}', 42)
                  """);

            await client.RequestBackfillAsync(table);

            await using var cmd = pg.DataSource.CreateCommand(
                "SELECT status, transform_version, cursor_json, rows_copied FROM wallaby.backfill_state WHERE table_qualified = $1");
            cmd.Parameters.AddWithValue(table);
            await using var reader = await cmd.ExecuteReaderAsync();
            (await reader.ReadAsync()).ShouldBeTrue();
            reader.GetString(0).ShouldBe("Requested");
            reader.GetString(1).ShouldBe("v2");        // preserved: the host compares it to the deployed version
            reader.IsDBNull(2).ShouldBeTrue();
            reader.GetInt64(3).ShouldBe(0);

            var status = await client.GetBackfillStatusAsync();
            var entry = status.Single(s => s.Table == table);
            entry.Status.ShouldBe(WallabyBackfillStatus.Requested);
            entry.RowsCopied.ShouldBe(0);
        }
        finally
        {
            await ExecAsync($"DELETE FROM wallaby.backfill_state WHERE table_qualified = '{table}'");
        }
    }

    [Test]
    public async Task Request_for_a_table_without_a_row_inserts_a_fresh_request()
    {
        var table = $"public.labels_{Guid.NewGuid():N}";
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureBackfillTableAsync();

            await client.RequestBackfillAsync(table);

            var status = await client.GetBackfillStatusAsync();
            status.Single(s => s.Table == table).Status.ShouldBe(WallabyBackfillStatus.Requested);
        }
        finally
        {
            await ExecAsync($"DELETE FROM wallaby.backfill_state WHERE table_qualified = '{table}'");
        }
    }

    [Test]
    public async Task Purge_request_sets_the_flag_and_survives_a_racing_plain_request()
    {
        var table = $"public.orders_{Guid.NewGuid():N}";
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureBackfillTableAsync();

            await client.RequestBackfillAsync(table, purge: true);
            (await ReadPurgeAsync(table)).ShouldBeTrue();

            // Sticky-OR: a plain request does not clear a pending purge.
            await client.RequestBackfillAsync(table);
            (await ReadPurgeAsync(table)).ShouldBeTrue();
        }
        finally
        {
            await ExecAsync($"DELETE FROM wallaby.backfill_state WHERE table_qualified = '{table}'");
        }
    }

    private async Task<bool> ReadPurgeAsync(string table)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            "SELECT purge FROM wallaby.backfill_state WHERE table_qualified = $1");
        cmd.Parameters.AddWithValue(table);
        return (bool)(await cmd.ExecuteScalarAsync())!;
    }

    [Test]
    public async Task Cancel_withdraws_a_queued_request_and_clears_its_purge_mark()
    {
        var table = $"public.orders_{Guid.NewGuid():N}";
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureBackfillTableAsync();

            await client.RequestBackfillAsync(table, purge: true);

            (await client.CancelBackfillAsync(table)).ShouldBeTrue();

            var status = await client.GetBackfillStatusAsync();
            status.Single(s => s.Table == table).Status.ShouldBe(WallabyBackfillStatus.Cancelled);
            (await ReadPurgeAsync(table)).ShouldBeFalse();

            // Nothing left to withdraw; the second cancel reports that.
            (await client.CancelBackfillAsync(table)).ShouldBeFalse();
        }
        finally
        {
            await ExecAsync($"DELETE FROM wallaby.backfill_state WHERE table_qualified = '{table}'");
        }
    }

    [Test]
    public async Task Cancel_does_not_touch_a_running_backfill()
    {
        var table = $"public.orders_{Guid.NewGuid():N}";
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureBackfillTableAsync();
            await ExecAsync(
                $"""
                 INSERT INTO wallaby.backfill_state (table_qualified, status, transform_version, cursor_json, rows_copied)
                 VALUES ('{table}', 'InProgress', 'v1', NULL, 10)
                 """);

            (await client.CancelBackfillAsync(table)).ShouldBeFalse();

            var status = await client.GetBackfillStatusAsync();
            status.Single(s => s.Table == table).Status.ShouldBe(WallabyBackfillStatus.InProgress);
        }
        finally
        {
            await ExecAsync($"DELETE FROM wallaby.backfill_state WHERE table_qualified = '{table}'");
        }
    }

    [Test]
    public async Task A_database_wallaby_never_touched_reads_empty_and_refuses_requests()
    {
        await ExecAsync("CREATE DATABASE backfill_virgin");
        var builder = new NpgsqlConnectionStringBuilder(pg.ConnectionString) { Database = "backfill_virgin" };
        await using var client = new WallabyControlClient(builder.ConnectionString);

        (await client.GetBackfillStatusAsync()).ShouldBeEmpty();
        await Should.ThrowAsync<InvalidOperationException>(() => client.RequestBackfillAsync("public.orders"));
        // Cancel has nothing to withdraw, so it reports false instead of throwing.
        (await client.CancelBackfillAsync("public.orders")).ShouldBeFalse();
    }
}
