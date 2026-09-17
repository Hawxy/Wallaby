using Microsoft.Extensions.Logging.Abstractions;
using Wallaby.Internal.Backfill;
using Wallaby.Model;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

/// <summary>The backfill row estimate comes from planner statistics, summed over a table's partitions.</summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class BackfillRowEstimatorTests(PostgresFixture pg)
{
    private static CapturedTable Table(string name) => new()
    {
        EntityClrType = typeof(object),
        Schema = "public",
        TableName = name,
        Columns = [],
        PrimaryKey = [],
    };

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    [Test]
    public async Task An_analysed_table_reports_its_row_count()
    {
        var name = $"est_{Guid.NewGuid():N}";
        await ExecAsync($"CREATE TABLE {name} (id int PRIMARY KEY); INSERT INTO {name} SELECT generate_series(1, 1234); ANALYZE {name};");
        try
        {
            var estimate = await new PostgresBackfillRowEstimator(pg.DataSource, NullLogger.Instance)
                .EstimateAsync(Table(name), CancellationToken.None);

            estimate.ShouldBe(1234);
        }
        finally
        {
            await ExecAsync($"DROP TABLE {name}");
        }
    }

    [Test]
    public async Task A_partitioned_table_sums_its_partitions()
    {
        var name = $"est_{Guid.NewGuid():N}";
        await ExecAsync(
            $"""
             CREATE TABLE {name} (id int, region text, PRIMARY KEY (id, region)) PARTITION BY LIST (region);
             CREATE TABLE {name}_eu PARTITION OF {name} FOR VALUES IN ('eu');
             CREATE TABLE {name}_us PARTITION OF {name} FOR VALUES IN ('us');
             INSERT INTO {name} SELECT g, 'eu' FROM generate_series(1, 300) g;
             INSERT INTO {name} SELECT g, 'us' FROM generate_series(1, 200) g;
             ANALYZE {name}_eu; ANALYZE {name}_us;
             """);
        try
        {
            var estimate = await new PostgresBackfillRowEstimator(pg.DataSource, NullLogger.Instance)
                .EstimateAsync(Table(name), CancellationToken.None);

            estimate.ShouldBe(500);
        }
        finally
        {
            await ExecAsync($"DROP TABLE {name}");
        }
    }

    [Test]
    public async Task A_never_analysed_table_or_a_missing_one_is_unknown()
    {
        var name = $"est_{Guid.NewGuid():N}";
        await ExecAsync($"CREATE TABLE {name} (id int PRIMARY KEY)");
        try
        {
            var estimator = new PostgresBackfillRowEstimator(pg.DataSource, NullLogger.Instance);

            (await estimator.EstimateAsync(Table(name), CancellationToken.None)).ShouldBeNull();
            (await estimator.EstimateAsync(Table("does_not_exist"), CancellationToken.None)).ShouldBeNull();
        }
        finally
        {
            await ExecAsync($"DROP TABLE {name}");
        }
    }
}
