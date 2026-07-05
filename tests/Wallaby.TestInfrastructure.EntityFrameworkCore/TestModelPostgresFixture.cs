using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.TestInfrastructure.EntityFrameworkCore;

/// <summary>
/// A <see cref="PostgresFixture"/> that also creates the <see cref="AppDbContext"/> application schema,
/// for suites that capture/replicate the EF Core test model.
/// </summary>
public sealed class TestModelPostgresFixture : PostgresFixture
{
    protected override async Task BootstrapAsync(string connectionString)
    {
        await using var ctx = new AppDbContext(TestModelFactory.CreateOptions(connectionString));
        await ctx.Database.EnsureCreatedAsync();
    }
}
