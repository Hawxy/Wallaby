using Wallaby.Internal;

namespace Wallaby.UnitTests;

public class RetryBackoffTests
{
    [Test]
    public void First_delay_is_about_the_base()
    {
        var backoff = new RetryBackoff(TimeSpan.FromSeconds(1));

        var first = backoff.Next();

        first.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(800));
        first.ShouldBeLessThanOrEqualTo(TimeSpan.FromMilliseconds(1200));
    }

    [Test]
    public void Delay_grows_and_is_capped()
    {
        var backoff = new RetryBackoff(TimeSpan.FromSeconds(1));

        var last = TimeSpan.Zero;
        for (var i = 0; i < 12; i++)
        {
            last = backoff.Next();
            last.ShouldBeLessThanOrEqualTo(TimeSpan.FromMinutes(2) * 1.2); // never past the cap (+jitter)
        }

        last.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromMinutes(2) * 0.8); // pinned at the cap after many attempts
    }

    [Test]
    public void Reset_returns_to_the_base()
    {
        var backoff = new RetryBackoff(TimeSpan.FromSeconds(1));
        for (var i = 0; i < 5; i++)
        {
            backoff.Next();
        }

        backoff.Reset();

        backoff.Next().ShouldBeLessThanOrEqualTo(TimeSpan.FromMilliseconds(1200));
    }
}
