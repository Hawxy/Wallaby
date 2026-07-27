using Npgsql;
using Wallaby.Internal.Replication;

namespace Wallaby.Tests.Unit;

/// <summary>
/// The replication connection string's handling: Npgsql rejects multi-host replication connections, so
/// <see cref="ReplicationPrimaryResolver"/> parses the host list for probing (single-host strings pass
/// through untouched), and the stream applies PerInstance array nullability unless configured.
/// </summary>
public class ReplicationConnectionStringTests
{
    [Test]
    public async Task A_single_host_string_is_returned_untouched_without_probing()
    {
        // An unreachable host proves no probe ran: resolution must be instant and identical.
        var connectionString = "Host=unreachable.invalid;Username=u;Password=s3cret;Database=d";

        var resolved = await ReplicationPrimaryResolver.ResolveAsync(connectionString, CancellationToken.None);

        resolved.ShouldBeSameAs(connectionString);
    }

    [Test]
    public void Host_entries_split_with_the_default_port()
    {
        ReplicationPrimaryResolver.ParseHosts("one,two", 5432)
            .ShouldBe([("one", 5432), ("two", 5432)]);
    }

    [Test]
    public void Host_entries_carry_their_own_ports()
    {
        ReplicationPrimaryResolver.ParseHosts("one:5433, two:5434 ,three", 5432)
            .ShouldBe([("one", 5433), ("two", 5434), ("three", 5432)]);
    }

    [Test]
    public void Bracketed_ipv6_entries_parse_with_and_without_ports()
    {
        ReplicationPrimaryResolver.ParseHosts("[::1]:5433,[2001:db8::2]", 5432)
            .ShouldBe([("::1", 5433), ("2001:db8::2", 5432)]);
    }

    [Test]
    public void An_unbracketed_ipv6_entry_is_a_host_without_a_port()
    {
        ReplicationPrimaryResolver.ParseHosts("2001:db8::2", 5432)
            .ShouldBe([("2001:db8::2", 5432)]);
    }

    [Test]
    public async Task An_unreachable_multi_host_string_fails_naming_the_hosts()
    {
        var connectionString =
            "Host=unreachable-one.invalid,unreachable-two.invalid;Username=u;Password=p;Database=d;Timeout=1";

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => ReplicationPrimaryResolver.ResolveAsync(connectionString, CancellationToken.None));

        ex.Message.ShouldContain("unreachable-one.invalid");
        ex.Message.ShouldContain("primary");
    }
}
