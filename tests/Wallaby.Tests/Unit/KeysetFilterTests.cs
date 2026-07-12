using Wallaby.Internal.Backfill;

namespace Wallaby.Tests.Unit;

public class KeysetFilterTests
{
    private static object?[][] SingleColumnTuples(params object?[] values)
        => values.Select(v => new[] { v }).ToArray();

    [Test]
    public void Single_column_lookup_binds_one_typed_array_parameter()
    {
        var filters = KeysetFilter.ForLookup(["category_id"], SingleColumnTuples(1, 2, 3));

        filters.Count.ShouldBe(1);
        filters[0].PredicateSql.ShouldBe("\"category_id\" = ANY(@f0)");
        filters[0].Parameters.Count.ShouldBe(1);
        filters[0].Parameters[0].ShouldBe(new[] { 1, 2, 3 });
    }

    [Test]
    public void Single_column_lookup_with_five_thousand_values_still_binds_one_parameter()
    {
        var tuples = SingleColumnTuples(Enumerable.Range(0, 5000).Cast<object?>().ToArray());

        var filters = KeysetFilter.ForLookup(["id"], tuples);

        filters.Count.ShouldBe(1);
        filters[0].Parameters.Count.ShouldBe(1);
        filters[0].Parameters[0].ShouldBeOfType<int[]>().Length.ShouldBe(5000);
    }

    [Test]
    public void Single_column_guid_values_bind_as_a_guid_array()
    {
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();

        var filters = KeysetFilter.ForLookup(["tenant_id"], SingleColumnTuples(a, b));

        filters[0].Parameters[0].ShouldBe(new[] { a, b });
    }

    [Test]
    public void Single_column_null_values_are_dropped()
    {
        var filters = KeysetFilter.ForLookup(["category_id"], SingleColumnTuples(1, null, 2));

        filters[0].Parameters[0].ShouldBe(new[] { 1, 2 });
    }

    [Test]
    public void Single_column_with_only_null_values_never_matches()
    {
        var filters = KeysetFilter.ForLookup(["category_id"], SingleColumnTuples(null, null));

        filters.Count.ShouldBe(1);
        filters[0].PredicateSql.ShouldBe("false");
        filters[0].Parameters.ShouldBeEmpty();
    }

    [Test]
    public void Single_column_element_type_without_an_array_mapping_falls_back_to_a_parameter_list()
    {
        // char has no typed-array mapping in the switch; the row-value fallback binds per value.
        var filters = KeysetFilter.ForLookup(["kind"], SingleColumnTuples('a', 'b'));

        filters.Count.ShouldBe(1);
        filters[0].Parameters.Count.ShouldBe(2);
        filters[0].PredicateSql.ShouldContain("IN (");
    }

    [Test]
    public void Composite_lookup_below_the_budget_produces_one_row_value_filter()
    {
        var tuples = new object?[][] { [1, "a"], [2, "b"], [3, "c"] };

        var filters = KeysetFilter.ForLookup(["x", "y"], tuples);

        filters.Count.ShouldBe(1);
        filters[0].PredicateSql.ShouldBe("(\"x\", \"y\") IN ((@f0, @f1), (@f2, @f3), (@f4, @f5))");
        filters[0].Parameters.ShouldBe([1, "a", 2, "b", 3, "c"]);
    }

    [Test]
    public void Composite_lookup_splits_into_batches_bounded_by_the_parameter_budget()
    {
        var tuples = new object?[][] { [1, "a"], [2, "b"], [3, "c"], [4, "d"], [5, "e"] };

        // Budget 4 with 2 columns => 2 tuples per filter => 3 filters (2 + 2 + 1).
        var filters = KeysetFilter.ForLookup(["x", "y"], tuples, maxParametersPerQuery: 4);

        filters.Count.ShouldBe(3);
        filters[0].Parameters.ShouldBe([1, "a", 2, "b"]);
        filters[1].Parameters.ShouldBe([3, "c", 4, "d"]);
        filters[2].Parameters.ShouldBe([5, "e"]);

        // Placeholders restart per filter, matching the pager's positional binding.
        filters[1].PredicateSql.ShouldBe("(\"x\", \"y\") IN ((@f0, @f1), (@f2, @f3))");
        filters[2].PredicateSql.ShouldBe("(\"x\", \"y\") IN ((@f0, @f1))");
    }

    [Test]
    public void A_budget_smaller_than_one_tuple_still_emits_one_tuple_per_batch()
    {
        var tuples = new object?[][] { [1, "a", true], [2, "b", false] };

        var filters = KeysetFilter.ForLookup(["x", "y", "z"], tuples, maxParametersPerQuery: 2);

        filters.Count.ShouldBe(2);
        filters[0].Parameters.Count.ShouldBe(3);
        filters[1].Parameters.Count.ShouldBe(3);
    }
}
