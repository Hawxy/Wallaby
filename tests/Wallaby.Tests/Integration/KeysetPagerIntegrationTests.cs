using Npgsql;
using Wallaby.Internal;
using Wallaby.Internal.Backfill;
using Wallaby.Model;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class KeysetPagerIntegrationTests(PostgresFixture pg)
{
    [Test]
    public async Task Any_array_filter_returns_exactly_the_matching_rows()
    {
        var tenants = Enumerable.Range(0, 30).Select(_ => Guid.NewGuid()).ToArray();
        await PgExec.ExecuteAsync(
            pg.DataSource,
            """
            DROP TABLE IF EXISTS wallaby_filter_single;
            CREATE TABLE wallaby_filter_single (id int PRIMARY KEY, tenant uuid NOT NULL, name text NOT NULL);
            INSERT INTO wallaby_filter_single (id, tenant, name)
            SELECT g, (@tenants)[(g % 30) + 1], 'n' || g FROM generate_series(1, 300) g;
            """,
            CancellationToken.None,
            ("tenants", tenants));

        // 12 present tenants plus 3 absent ones; the pager must return exactly the 12 tenants' rows.
        var lookup = tenants.Take(12).Concat([Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()])
            .Select(t => new object?[] { t })
            .ToArray();
        var expected = Enumerable.Range(1, 300).Where(g => g % 30 < 12).ToHashSet();

        var filters = KeysetFilter.ForLookup(["tenant"], lookup);
        filters.Count.ShouldBe(1);

        var ids = await ReadAllIdsAsync(Table("wallaby_filter_single", ("tenant", typeof(Guid)), ("name", typeof(string))), filters, chunkSize: 50);

        ids.Count.ShouldBe(expected.Count); // no duplicates across pages
        ids.ToHashSet().SetEquals(expected).ShouldBeTrue();
    }

    [Test]
    public async Task Batched_composite_filters_union_to_the_exact_matching_set()
    {
        await PgExec.ExecuteAsync(
            pg.DataSource,
            """
            DROP TABLE IF EXISTS wallaby_filter_pairs;
            CREATE TABLE wallaby_filter_pairs (id int PRIMARY KEY, a int NOT NULL, b text NOT NULL);
            INSERT INTO wallaby_filter_pairs (id, a, b)
            SELECT g, g % 10, 'b' || (g % 5) FROM generate_series(1, 60) g;
            """,
            CancellationToken.None);

        var pairs = new (int A, string B)[] { (0, "b0"), (1, "b1"), (2, "b2"), (7, "b2"), (9, "b4") };
        var lookup = pairs.Select(p => new object?[] { p.A, p.B }).ToArray();
        var expected = Enumerable.Range(1, 60)
            .Where(g => pairs.Contains((g % 10, "b" + (g % 5))))
            .ToHashSet();

        // Budget 4 with 2 columns => 2 tuples per filter => 3 filters, each scanned separately.
        var filters = KeysetFilter.ForLookup(["a", "b"], lookup, maxParametersPerQuery: 4);
        filters.Count.ShouldBe(3);

        var ids = await ReadAllIdsAsync(Table("wallaby_filter_pairs", ("a", typeof(int)), ("b", typeof(string))), filters, chunkSize: 7);

        ids.Count.ShouldBe(expected.Count); // no duplicates across filters or pages
        ids.ToHashSet().SetEquals(expected).ShouldBeTrue();
    }

    private async Task<List<int>> ReadAllIdsAsync(CapturedTable table, IReadOnlyList<KeysetFilter> filters, int chunkSize)
    {
        var ids = new List<int>();
        await using var connection = await pg.DataSource.OpenConnectionAsync();
        foreach (var filter in filters)
        {
            var pager = new KeysetPager(table, filter);
            object?[]? cursor = null;
            while (true)
            {
                var chunk = await pager.ReadChunkAsync(connection, cursor, chunkSize, CancellationToken.None);
                foreach (var row in chunk.Rows)
                {
                    ids.Add((int)row.NewValues!.Single(c => c.ColumnName == "id").Value!);
                }
                if (!chunk.HasMore)
                {
                    break;
                }
                cursor = chunk.NextCursor;
            }
        }
        return ids;
    }

    private static CapturedTable Table(string name, params (string Column, Type ClrType)[] extraColumns)
    {
        var id = new CapturedColumn { PropertyName = "Id", ColumnName = "id", ClrType = typeof(int), IsPrimaryKey = true };
        var columns = new List<CapturedColumn> { id };
        columns.AddRange(extraColumns.Select(c => new CapturedColumn
        {
            PropertyName = c.Column,
            ColumnName = c.Column,
            ClrType = c.ClrType,
            IsPrimaryKey = false,
        }));

        return new CapturedTable
        {
            EntityClrType = typeof(object),
            Schema = "public",
            TableName = name,
            Columns = columns,
            PrimaryKey = [id],
        };
    }
}
