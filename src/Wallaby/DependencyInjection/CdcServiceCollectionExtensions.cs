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
    /// Add Postgres CDC. Supply a connection string via <c>cdc.UseConnectionString(...)</c>. For capture
    /// (any sink, <c>Map&lt;T&gt;()</c>, or <c>CaptureAllMappedTables()</c>) also declare the driving
    /// <c>DbContext</c> with <c>cdc.UseContext&lt;TContext&gt;()</c> and register an
    /// <see cref="IDbContextFactory{TContext}"/>. If only external slots are declared (no capture), Wallaby
    /// runs provision-only: it creates/reconciles those slots and never opens a primary slot or streams.
    /// CDC owns a pooled <c>NpgsqlDataSource</c> built from the connection string for all non-replication work.
    /// </summary>
    public static IServiceCollection AddWallaby(this IServiceCollection services, Action<CdcBuilder> configure)
    {
        var builder = new CdcBuilder();
        configure(builder);
        var configuration = builder.Build();

        services.AddSingleton(configuration);
        services.AddSingleton(configuration.Options);

        services.AddSingleton(_ => new CdcDataSource(configuration.Options.ConnectionString));

        services.AddMetrics();
        services.AddSingleton(sp => new WallabyInstrumentation(sp.GetRequiredService<IMeterFactory>()));

        // Live node status surface (role, progress, faults) — read by diagnostics and health checks.
        services.AddSingleton(_ => new CdcStatus(
            configuration.CaptureIntended ? configuration.Options.SlotName : "", TimeProvider.System));
        services.AddSingleton<ICdcStatus>(sp => sp.GetRequiredService<CdcStatus>());

        services.AddSingleton<IClusterLock>(sp =>
            new Internal.Cluster.PostgresAdvisoryLock(
                sp.GetRequiredService<CdcDataSource>().Source, configuration.Options.LeaderHeartbeatInterval));

        if (configuration.CaptureIntended)
        {
            // Registers the generic capture runtime for the context declared via UseContext<TContext>().
            configuration.RegisterCaptureRuntime!(services);
        }
        else
        {
            // Provision-only: create the declared external slots (if any) and idle — no primary slot/stream.
            services.AddSingleton<IHostedService, ExternalSlotProvisioningService>();
        }

        return services;
    }

    /// <summary>
    /// Registers the capture runtime (backfill manager, <see cref="CdcRuntime{TContext}"/>, and its hosted
    /// service) for <typeparamref name="TContext"/>. Invoked through the delegate captured by
    /// <see cref="CdcBuilder.UseContext{TContext}"/>, so the generic type stays out of the public entry point.
    /// </summary>
    internal static void RegisterCaptureRuntime<TContext>(IServiceCollection services) where TContext : DbContext
    {
        // Resolve the capture model once; both the runtime and the backfill manager share this instance.
        services.AddSingleton(sp =>
        {
            using var context = sp.GetRequiredService<IDbContextFactory<TContext>>().CreateDbContext();
            var efModel = context.Model;
            var cdc = ModelToCdcModel.Build(efModel, sp.GetRequiredService<CdcConfiguration>().ToCaptureSpec());
            return new CapturedModel(efModel, cdc);
        });

        services.AddSingleton<ICdcBackfillManager>(sp =>
            new DefaultBackfillManager(
                sp.GetRequiredService<CapturedModel>().Cdc,
                new PostgresBackfillStore(sp.GetRequiredService<CdcDataSource>().Source)));

        services.AddSingleton<CdcRuntime<TContext>>();
        services.AddSingleton<IHostedService, CdcBackgroundService<TContext>>();
    }
}
