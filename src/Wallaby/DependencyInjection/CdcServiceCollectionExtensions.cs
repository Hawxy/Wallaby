using System.Diagnostics.Metrics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Hosting;
using Wallaby.Internal;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.SelfConfig;
using Wallaby.Internal.State;

namespace Wallaby.DependencyInjection;

/// <summary>Entry point for adding Wallaby to a host.</summary>
public static class CdcServiceCollectionExtensions
{
    /// <summary>
    /// Add Postgres CDC driven by <typeparamref name="TContext"/>. The consumer must also register
    /// an <see cref="IDbContextFactory{TContext}"/> (e.g. via <c>AddDbContextFactory&lt;TContext&gt;</c>)
    /// and supply a connection string via <c>cdc.UseConnectionString(...)</c>. CDC owns a pooled
    /// <c>NpgsqlDataSource</c> built from that connection string for all of its non-replication work.
    /// </summary>
    public static IServiceCollection AddWallaby<TContext>(this IServiceCollection services, Action<CdcBuilder> configure)
        where TContext : DbContext
    {
        var builder = new CdcBuilder();
        configure(builder);
        var configuration = builder.Build();

        services.AddSingleton(configuration);
        services.AddSingleton(configuration.Options);

        services.AddSingleton(_ => new CdcDataSource(configuration.Options.ConnectionString));
        
        services.AddMetrics();
        services.AddSingleton(sp => new WallabyInstrumentation(sp.GetRequiredService<IMeterFactory>()));

        services.AddSingleton<IClusterLock>(sp =>
            new Internal.Cluster.PostgresAdvisoryLock(
                sp.GetRequiredService<CdcDataSource>().Source, configuration.Options.LeaderHeartbeatInterval));

        services.AddSingleton<ICdcBackfillManager>(sp =>
        {
            var factory = sp.GetRequiredService<IDbContextFactory<TContext>>();
            using var context = factory.CreateDbContext();
            var model = ModelToCdcModel.Build(context.Model, ToCaptureSpec(configuration));
            return new DefaultBackfillManager(
                model, new PostgresBackfillStore(sp.GetRequiredService<CdcDataSource>().Source));
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
