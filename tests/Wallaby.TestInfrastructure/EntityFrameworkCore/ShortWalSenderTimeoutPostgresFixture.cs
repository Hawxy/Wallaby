using Wallaby.TestModel;

namespace Wallaby.TestInfrastructure.EntityFrameworkCore;

/// <summary>
/// A test-model container whose walsender disconnects after 2 seconds without a client status update —
/// for proving the keepalive path keeps slow deliveries alive.
/// </summary>
public sealed class ShortWalSenderTimeoutPostgresFixture() : PostgresFixture(["wal_sender_timeout=2s"])
{
    protected override async Task BootstrapAsync(string connectionString)
    {
        await using var ctx = new AppDbContext(TestModelFactory.CreateOptions(connectionString));
        await ctx.Database.EnsureCreatedAsync();
    }
}
