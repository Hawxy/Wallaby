using Wallaby.Hosting;

namespace EFCore.CDC.UnitTests;

public class RetryBackoffTests
{
    [Test]
    public async Task First_delay_is_about_the_base()
    {
        var backoff = new RetryBackoff(TimeSpan.FromSeconds(1));

        var first = backoff.Next();

        await Assert.That(first).IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(800));
        await Assert.That(first).IsLessThanOrEqualTo(TimeSpan.FromMilliseconds(1200));
    }

    [Test]
    public async Task Delay_grows_and_is_capped()
    {
        var backoff = new RetryBackoff(TimeSpan.FromSeconds(1));

        var last = TimeSpan.Zero;
        for (var i = 0; i < 12; i++)
        {
            last = backoff.Next();
            await Assert.That(last).IsLessThanOrEqualTo(TimeSpan.FromMinutes(2) * 1.2); // never past the cap (+jitter)
        }

        await Assert.That(last).IsGreaterThanOrEqualTo(TimeSpan.FromMinutes(2) * 0.8); // pinned at the cap after many attempts
    }

    [Test]
    public async Task Reset_returns_to_the_base()
    {
        var backoff = new RetryBackoff(TimeSpan.FromSeconds(1));
        for (var i = 0; i < 5; i++)
        {
            backoff.Next();
        }

        backoff.Reset();

        await Assert.That(backoff.Next()).IsLessThanOrEqualTo(TimeSpan.FromMilliseconds(1200));
    }
}
