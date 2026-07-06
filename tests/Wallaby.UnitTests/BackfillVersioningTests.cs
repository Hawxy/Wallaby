using Wallaby.Internal.Backfill;

namespace Wallaby.UnitTests;

/// <summary>
/// The per-table composite backfill version: any mapping's version bump must change it, while
/// declaration order and duplicate declarations must not.
/// </summary>
public class BackfillVersioningTests
{
    [Test]
    public void No_declared_versions_compose_to_null()
    {
        BackfillVersioning.Compose([]).ShouldBeNull();
    }

    [Test]
    public void A_single_version_passes_through_unchanged()
    {
        BackfillVersioning.Compose(["v1"]).ShouldBe("v1");
    }

    [Test]
    public void Composition_is_order_insensitive_and_dedupes()
    {
        BackfillVersioning.Compose(["v2", "v1", "v2"]).ShouldBe("v1+v2");
        BackfillVersioning.Compose(["v1", "v2"]).ShouldBe("v1+v2");
    }

    [Test]
    public void Bumping_one_mappings_version_changes_the_composite()
    {
        BackfillVersioning.Compose(["v1", "v2"]).ShouldNotBe(BackfillVersioning.Compose(["v1", "v3"]));
    }
}
