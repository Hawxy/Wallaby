namespace Wallaby.Client.Tests.Unit;

public class WallabyBackfillStatusContractTests
{
    [Test]
    public void Member_names_are_pinned_to_the_persisted_strings()
    {
        // Parsed from the strings the host's BackfillStatus persists in wallaby.backfill_state
        // (whose pin test carries the same list): never rename.
        Enum.GetNames<WallabyBackfillStatus>()
            .ShouldBe(["Requested", "InProgress", "Completed", "Cancelled", "Unknown"]);
    }

    [Test]
    public void Persisted_names_this_client_does_not_know_read_as_unknown()
    {
        WallabyControlClient.ParseBackfillStatus("Completed").ShouldBe(WallabyBackfillStatus.Completed);
        WallabyControlClient.ParseBackfillStatus("Paused").ShouldBe(WallabyBackfillStatus.Unknown);
        WallabyControlClient.ParseSlotKind("external").ShouldBe(WallabyManagedSlotKind.External);
        WallabyControlClient.ParseSlotKind("failover").ShouldBe(WallabyManagedSlotKind.Unknown);
    }
}
