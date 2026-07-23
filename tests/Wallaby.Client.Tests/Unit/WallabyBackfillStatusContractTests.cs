namespace Wallaby.Client.Tests.Unit;

public class WallabyBackfillStatusContractTests
{
    [Test]
    public void Member_names_are_pinned_to_the_persisted_strings()
    {
        // Parsed from the strings the host's BackfillStatus persists in wallaby.backfill_state
        // (whose pin test carries the same list): never rename.
        Enum.GetNames<WallabyBackfillStatus>()
            .ShouldBe(["NotStarted", "Requested", "InProgress", "Completed"]);
    }
}
