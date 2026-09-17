namespace Wallaby.Client.Tests.Unit;

/// <summary>Progress and ETA are derived from the persisted estimate, start time and last update.</summary>
public class WallabyBackfillStateProgressTests
{
    private static readonly DateTimeOffset Started = new(2026, 9, 17, 10, 0, 0, TimeSpan.Zero);

    [Test]
    public void Progress_and_remaining_time_follow_the_average_rate()
    {
        var state = new WallabyBackfillState("public.orders", WallabyBackfillStatus.InProgress, 250, Started.AddMinutes(5))
        {
            EstimatedRows = 1_000,
            StartedAt = Started,
        };

        state.Progress.ShouldBe(0.25);
        state.EstimatedRemaining.ShouldBe(TimeSpan.FromMinutes(15));
    }

    [Test]
    public void Progress_is_capped_and_remaining_is_null_without_the_facts()
    {
        var over = new WallabyBackfillState("t", WallabyBackfillStatus.InProgress, 1_200, Started.AddMinutes(1))
        {
            EstimatedRows = 1_000,
            StartedAt = Started,
        };
        over.Progress.ShouldBe(1);
        over.EstimatedRemaining.ShouldBe(TimeSpan.Zero);

        new WallabyBackfillState("t", WallabyBackfillStatus.InProgress, 10, Started.AddMinutes(1)).Progress.ShouldBeNull();
        new WallabyBackfillState("t", WallabyBackfillStatus.InProgress, 10, Started.AddMinutes(1)).EstimatedRemaining.ShouldBeNull();
        new WallabyBackfillState("t", WallabyBackfillStatus.Completed, 500, Started.AddMinutes(1))
        {
            EstimatedRows = 1_000, StartedAt = Started,
        }.EstimatedRemaining.ShouldBeNull();
        new WallabyBackfillState("t", WallabyBackfillStatus.InProgress, 0, Started.AddMinutes(1))
        {
            EstimatedRows = 1_000, StartedAt = Started,
        }.EstimatedRemaining.ShouldBeNull();
    }
}
