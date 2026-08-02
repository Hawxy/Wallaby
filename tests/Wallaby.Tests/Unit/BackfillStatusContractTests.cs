using Wallaby.Abstractions;

namespace Wallaby.Tests.Unit;

public class BackfillStatusContractTests
{
    [Test]
    public void Member_names_are_pinned_to_the_persisted_strings()
    {
        // Persisted by name in wallaby.backfill_state and parsed back by the host and by
        // Wallaby.Client's WallabyBackfillStatus (which pins the same list): never rename.
        Enum.GetNames<BackfillStatus>()
            .ShouldBe(["NotStarted", "Requested", "InProgress", "Completed", "Cancelled"]);
    }
}
