using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.EntityFrameworkCore.Internal;
using Wallaby.Internal.Pipeline;
using Wallaby.TestModel;

namespace Wallaby.EntityFrameworkCore.UnitTests;

/// <summary>
/// <see cref="DbContextResolver"/> obtains the consumer's context without requiring an
/// <see cref="IDbContextFactory{TContext}"/> — it uses a registered factory when present and otherwise a DI
/// scope over a plain <c>AddDbContext</c>. (Reading <c>.Model</c> never connects, so no database is needed.)
/// </summary>
public class DbContextResolverTests
{
    [Test]
    public async Task ReadModel_resolves_via_scoped_AddDbContext_without_a_factory()
    {
        var services = new ServiceCollection();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(TestModelFactory.ModelOnlyConnectionString));
        await using var sp = services.BuildServiceProvider();

        var model = DbContextResolver.ReadModel<AppDbContext>(sp);

        model.FindEntityType(typeof(Product)).ShouldNotBeNull();
    }

    [Test]
    public async Task ReadModel_resolves_via_a_registered_factory()
    {
        var services = new ServiceCollection();
        services.AddDbContextFactory<AppDbContext>(o => o.UseNpgsql(TestModelFactory.ModelOnlyConnectionString));
        await using var sp = services.BuildServiceProvider();

        var model = DbContextResolver.ReadModel<AppDbContext>(sp);

        model.FindEntityType(typeof(Product)).ShouldNotBeNull();
    }

    [Test]
    public async Task Lease_yields_a_context_from_scoped_AddDbContext()
    {
        var services = new ServiceCollection();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(TestModelFactory.ModelOnlyConnectionString));
        await using var sp = services.BuildServiceProvider();

        await using var lease = DbContextResolver.Lease<AppDbContext>(sp);

        lease.Context.ShouldNotBeNull();
    }

    [Test]
    public async Task Lease_yields_a_context_from_a_registered_factory()
    {
        var services = new ServiceCollection();
        services.AddDbContextFactory<AppDbContext>(o => o.UseNpgsql(TestModelFactory.ModelOnlyConnectionString));
        await using var sp = services.BuildServiceProvider();

        await using var lease = DbContextResolver.Lease<AppDbContext>(sp);

        lease.Context.ShouldNotBeNull();
    }
}
