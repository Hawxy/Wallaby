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

    [Test]
    public void A_password_provider_rejects_a_password_in_the_connection_string()
    {
        var ex = Should.Throw<WallabyConfigurationException>(() => new WallabyDataSource(
            "Host=localhost;Username=u;Password=p", passwordProvider: _ => new ValueTask<string>("token")));

        ex.Message.ShouldContain("Password");
        ex.Message.ShouldContain("UsePasswordProvider");
    }

    [Test]
    public async Task Without_a_provider_the_replication_string_is_the_original()
    {
        var raw = "Host=localhost;Username=u;Password=p";
        await using var ds = new WallabyDataSource(raw);

        (await ds.ConnectionStringWithPasswordAsync(CancellationToken.None)).ShouldBeSameAs(raw);
    }

    [Test]
    public async Task The_replication_string_embeds_the_provided_token_verbatim()
    {
        const string token = "us-east-1:X-Amz-Algorithm=AWS4;Expires=900&'quote' space";
        var calls = 0;
        await using var ds = new WallabyDataSource(
            "Host=localhost;Username=u", passwordProvider: _ => { calls++; return new ValueTask<string>(token); });

        var authenticated = await ds.ConnectionStringWithPasswordAsync(CancellationToken.None);

        new NpgsqlConnectionStringBuilder(authenticated).Password.ShouldBe(token);
        new NpgsqlConnectionStringBuilder(authenticated).Host.ShouldBe("localhost");
        // One call for this string; the pool's periodic provider fetches on its own timer.
        calls.ShouldBeGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task An_empty_provided_password_is_rejected()
    {
        await using var ds = new WallabyDataSource("Host=localhost;Username=u", passwordProvider: _ => new ValueTask<string>(""));

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await ds.ConnectionStringWithPasswordAsync(CancellationToken.None));
        ex.Message.ShouldContain("no password");
    }

    [Test]
    public async Task A_hung_provider_times_out_on_the_replication_path()
    {
        await using var ds = new WallabyDataSource(
            "Host=localhost;Username=u",
            passwordProvider: async ct => { await Task.Delay(Timeout.InfiniteTimeSpan, ct); return "never"; },
            passwordFetchTimeout: TimeSpan.FromMilliseconds(100));

        var ex = await Should.ThrowAsync<TimeoutException>(
            async () => await ds.ConnectionStringWithPasswordAsync(CancellationToken.None));
        ex.Message.ShouldContain("password provider");
    }

    [Test]
    public async Task A_cancelled_caller_is_not_reported_as_a_timeout()
    {
        await using var ds = new WallabyDataSource(
            "Host=localhost;Username=u",
            passwordProvider: async ct => { await Task.Delay(Timeout.InfiniteTimeSpan, ct); return "never"; });
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Should.ThrowAsync<OperationCanceledException>(
            async () => await ds.ConnectionStringWithPasswordAsync(cts.Token));
    }

    [Test]
    public async Task Configure_data_source_runs_after_wallaby_settings()
    {
        await using var ds = new WallabyDataSource(
            "Host=localhost;Username=u", configureDataSource: b => b.ConnectionStringBuilder.ApplicationName = "hook");

        var builder = new NpgsqlConnectionStringBuilder(ds.Source.ConnectionString);
        builder.ApplicationName.ShouldBe("hook");
        builder.MaxAutoPrepare.ShouldBe(64);
    }
}
