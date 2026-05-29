using EFCore.CDC.Abstractions;
using EFCore.CDC.Hosting;
using EFCore.CDC.Internal.Backfill;
using EFCore.CDC.Internal.SelfConfig;
using EFCore.CDC.Internal.State;
using EFCore.CDC.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EFCore.CDC.DependencyInjection;

/// <summary>Entry point for adding EFCore.CDC to a host.</summary>
public static class CdcServiceCollectionExtensions
{
    /// <summary>
    /// Add Postgres CDC driven by <typeparamref name="TContext"/>. The consumer must also register an
    /// <see cref="IDbContextFactory{TContext}"/> (e.g. via <c>AddDbContextFactory&lt;TContext&gt;</c>).
    /// </summary>
    public static IServiceCollection AddCdc<TContext>(this IServiceCollection services, Action<CdcBuilder> configure)
        where TContext : DbContext
    {
        var builder = new CdcBuilder();
        configure(builder);
        var configuration = builder.Build();

        services.AddSingleton(configuration);
        services.AddSingleton(configuration.Options);

        services.AddSingleton<IClusterLock>(_ => new Internal.Cluster.PostgresAdvisoryLock(configuration.Options.ConnectionString));

        services.AddSingleton<ICdcBackfillManager>(sp =>
        {
            var factory = sp.GetRequiredService<IDbContextFactory<TContext>>();
            using var context = factory.CreateDbContext();
            var model = ModelToCdcModel.Build(context.Model, ToCaptureSpec(configuration));
            return new DefaultBackfillManager(model, new PostgresBackfillStore(configuration.Options.ConnectionString));
        });

        services.AddSingleton<CdcRuntime<TContext>>();
        services.AddSingleton<IHostedService, CdcBackgroundService<TContext>>();

        return services;
    }

    private static CaptureSpec ToCaptureSpec(CdcConfiguration configuration) => new()
    {
        CaptureAllMapped = configuration.CaptureAllMapped,
        DeclaredEntities = configuration.DeclaredEntities,
        RequiresFullReplicaIdentity = configuration.RequiresFullReplicaIdentity,
    };
}
