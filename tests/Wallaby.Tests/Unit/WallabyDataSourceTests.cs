using Npgsql;
using Wallaby.Internal;

namespace Wallaby.Tests.Unit;

public class WallabyDataSourceTests
{
    private static int MaxAutoPrepareOf(WallabyDataSource ds)
        => new NpgsqlConnectionStringBuilder(ds.Source.ConnectionString).MaxAutoPrepare;

    [Test]
    public async Task Auto_prepare_is_enabled_by_default()
    {
        await using var ds = new WallabyDataSource("Host=localhost;Username=u;Password=p");

        MaxAutoPrepareOf(ds).ShouldBe(64);
    }

    [Test]
    public async Task An_explicit_auto_prepare_setting_is_respected()
    {
        await using var ds = new WallabyDataSource("Host=localhost;Username=u;Max Auto Prepare=10");

        MaxAutoPrepareOf(ds).ShouldBe(10);
    }

    [Test]
    public async Task Explicitly_disabled_auto_prepare_stays_disabled()
    {
        await using var ds = new WallabyDataSource("Host=localhost;Username=u;Max Auto Prepare=0");

        MaxAutoPrepareOf(ds).ShouldBe(0);
    }

    [Test]
    public async Task The_raw_connection_string_is_preserved_for_the_replication_connection()
    {
        var raw = "Host=localhost;Username=u;Password=p";
        await using var ds = new WallabyDataSource(raw);

        ds.ConnectionString.ShouldBe(raw);
    }
}
