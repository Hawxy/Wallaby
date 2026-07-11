using Microsoft.EntityFrameworkCore;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Providers;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit;

/// <summary>
/// Declared TPH entities are rejected at capture-model build: hierarchy members share one table, so
/// rows would materialize as one arbitrary type and lose subclass data. TPT and TPC map each type to
/// its own table and stay capturable.
/// </summary>
public class TphHierarchyTests
{
    public class Animal
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class Cat : Animal
    {
        public int LivesLeft { get; set; }
    }

    private sealed class TphContext(DbContextOptions<TphContext> options) : DbContext(options)
    {
        public DbSet<Animal> Animals => Set<Animal>();
        public DbSet<Cat> Cats => Set<Cat>();
    }

    private sealed class TptContext(DbContextOptions<TptContext> options) : DbContext(options)
    {
        public DbSet<Animal> Animals => Set<Animal>();
        public DbSet<Cat> Cats => Set<Cat>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Animal>().UseTptMappingStrategy();
        }
    }

    private sealed class TpcContext(DbContextOptions<TpcContext> options) : DbContext(options)
    {
        public DbSet<Cat> Cats => Set<Cat>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Animal>().UseTpcMappingStrategy();
        }
    }

    private static TContext ModelOnly<TContext>() where TContext : DbContext
        => (TContext)Activator.CreateInstance(typeof(TContext), new DbContextOptionsBuilder<TContext>()
            .UseNpgsql(TestModelFactory.ModelOnlyConnectionString)
            .Options)!;

    private static CaptureSpec Declared(params Type[] types) => new() { DeclaredEntities = types };

    [Test]
    public async Task Declared_tph_root_fails_fast()
    {
        await using var ctx = ModelOnly<TphContext>();

        var ex = Should.Throw<WallabyConfigurationException>(
            () => EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Animal))));
        ex.Message.ShouldContain("TPH");
        ex.Message.ShouldContain(typeof(Animal).FullName!);
    }

    [Test]
    public async Task Declared_tph_leaf_fails_fast()
    {
        await using var ctx = ModelOnly<TphContext>();

        Should.Throw<WallabyConfigurationException>(
            () => EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Cat))));
    }

    [Test]
    public async Task Declared_tpt_type_is_not_rejected()
    {
        await using var ctx = ModelOnly<TptContext>();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Animal)));

        model.FindByClrType(typeof(Animal)).ShouldNotBeNull();
    }

    [Test]
    public async Task Declared_tpc_type_is_not_rejected()
    {
        await using var ctx = ModelOnly<TpcContext>();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Cat)));

        model.FindByClrType(typeof(Cat)).ShouldNotBeNull();
    }
}
