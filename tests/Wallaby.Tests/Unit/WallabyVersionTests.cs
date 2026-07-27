using Microsoft.Extensions.Logging.Testing;
using Wallaby.Hosting;
using Wallaby.Internal;

namespace Wallaby.Tests.Unit;

/// <summary>
/// The version stamped into startup logs and <c>wallaby.schema_version.applied_by</c>: the package
/// version, with the SDK-appended commit hash shortened.
/// </summary>
public class WallabyVersionTests
{
    [Test]
    public void Current_reports_the_package_version()
    {
        WallabyVersion.Current.ShouldNotBeNullOrWhiteSpace();
        WallabyVersion.Current.ShouldNotBe("unknown");
        WallabyVersion.Current.ShouldStartWith(
            typeof(WallabyVersion).Assembly.GetName().Version!.ToString(2)); // major.minor
    }

    [Test]
    public void A_commit_hash_is_shortened()
    {
        var plus = WallabyVersion.Current.IndexOf('+');
        if (plus < 0)
        {
            return; // built without a source revision (no git metadata)
        }

        (WallabyVersion.Current.Length - plus - 1).ShouldBe(7);
    }

    [Test]
    public void The_startup_log_names_the_version_slot_and_publication()
    {
        var logger = new FakeLogger();

        logger.RuntimeStarting(WallabyVersion.Current, "wallaby_slot", "wallaby_pub");

        var message = logger.LatestRecord.Message;
        message.ShouldContain(WallabyVersion.Current);
        message.ShouldContain("wallaby_slot");
        message.ShouldContain("wallaby_pub");
    }
}
