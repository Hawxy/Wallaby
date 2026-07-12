using Wallaby.Internal.Cluster;

namespace Wallaby.Tests.Unit;

public class AdvisoryLockKeyTests
{
    [Test]
    public void Stable_key_is_pinned_across_releases()
    {
        // Mixed-version nodes contend on this key: it must never change for the same name.
        PostgresAdvisoryLock.StableKey("wallaby_cdc").ShouldBe(-7866188437589534318L);
    }

    [Test]
    public void Different_names_produce_different_keys()
    {
        PostgresAdvisoryLock.StableKey("slot_a").ShouldNotBe(PostgresAdvisoryLock.StableKey("slot_b"));
    }
}
