using Wallaby.Internal.Backfill;
using Wallaby.Internal.State;

namespace Wallaby.Tests.Unit;

public class PostgresFanoutQueueStoreTests
{
    [Test]
    public void Canonical_values_json_and_hash_are_pinned()
    {
        // Pinned bytes: in-flight fanout_queue rows carry lookup_hash values derived from exactly this
        // canonical form, so it must never change. Note the ordinal sort runs over each tuple's JSON
        // (12 sorts before 1 because ']' > '2'), not over the values.
        var valuesJson = PostgresFanoutQueueStore.CanonicalValuesJson([[1], [12], [2]]);

        valuesJson.ShouldBe("[[12],[1],[2]]");
        PostgresFanoutQueueStore.Hash("public.orders", ["tenant_id"], valuesJson)
            .ShouldBe("97FDE398E6842E8E045ED559069904BBF003FBB96FEA39BCBC638DC45274A779");
    }

    [Test]
    public void Composite_tuples_with_awkward_strings_are_pinned()
    {
        var valuesJson = PostgresFanoutQueueStore.CanonicalValuesJson([[1, "b"], [1, "a]"], [null, "x"]]);

        valuesJson.ShouldBe("""[[1,"a]"],[1,"b"],[null,"x"]]""");
    }

    [Test]
    public void Canonical_json_matches_the_reference_algorithm()
    {
        var guid = Guid.Parse("8f8c8a4e-3c1d-4f2a-9b6e-2d5f7a1c9e33");
        var at = new DateTime(2026, 7, 5, 1, 2, 3, DateTimeKind.Utc);
        IReadOnlyList<object?[]> tuples =
        [
            [1, "plain"],
            [12, "pre]fix"],
            [1, "say \"hi\""],
            [2, "a,b"],
            [null, null],
            [guid, at],
            [12, null],
        ];

        PostgresFanoutQueueStore.CanonicalValuesJson(tuples).ShouldBe(Reference(tuples));
    }

    [Test]
    public void Encounter_order_does_not_change_the_canonical_json()
    {
        IReadOnlyList<object?[]> tuples = [[1, "x"], [12, "y"], [2, null], [null, "x"]];
        var expected = PostgresFanoutQueueStore.CanonicalValuesJson(tuples);

        PostgresFanoutQueueStore.CanonicalValuesJson([.. tuples.Reverse()]).ShouldBe(expected);
        PostgresFanoutQueueStore.CanonicalValuesJson([tuples[2], tuples[0], tuples[3], tuples[1]]).ShouldBe(expected);
    }

    // The pre-optimization canonicalization, kept as the equivalence oracle: sort tuples by their
    // single-tuple SerializeTuples form, then serialize the sorted set in one pass.
    private static string Reference(IReadOnlyList<object?[]> tuples)
    {
        var sorted = tuples
            .Select(t => (Tuple: t, Json: KeysetCodec.SerializeTuples([t])))
            .OrderBy(x => x.Json, StringComparer.Ordinal)
            .Select(x => x.Tuple)
            .ToList();
        return KeysetCodec.SerializeTuples(sorted);
    }
}
