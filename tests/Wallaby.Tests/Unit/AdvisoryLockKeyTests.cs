using Npgsql;
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

    [Test]
    [Arguments("Host=localhost", false, true)]
    [Arguments("Host=localhost", true, true)]
    [Arguments("Host=localhost;Keepalive=10", false, false)]
#if !NET11_0_OR_GREATER
    [Arguments("Host=localhost;Multiplexing=true", true, false)]
#endif
    public async Task Monitors_passively_unless_npgsql_keepalive_or_multiplexing_is_on(
        string connectionString, bool transactional, bool expected)
    {
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        new PostgresAdvisoryLock(dataSource, transactional).MonitorsPassively.ShouldBe(expected);
    }

#if !NET11_0_OR_GREATER
    [Test]
    public async Task Multiplexing_requires_the_transactional_lock()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Multiplexing=true");
        Should.Throw<NotSupportedException>(() => new PostgresAdvisoryLock(dataSource, transactional: false));
    }
#endif
}
