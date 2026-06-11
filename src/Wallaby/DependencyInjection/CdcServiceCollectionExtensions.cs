using System.Diagnostics.Metrics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
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
    /// <see cref="CdcOptions"/> participates in the standard options pipeline: <c>Configure&lt;CdcOptions&gt;</c>
    /// and configuration binding compose with the builder's <c>ConfigureOptions</c> in registration order, and
    /// <c>PostConfigure</c> runs last; option values are validated on first resolution.
    /// </summary>
    public static IServiceCollection AddWallaby(this IServiceCollection services, Action<CdcBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new CdcBuilder();
        configure(builder);
        services.AddSingleton(builder.Build());
        return AddWallabyCore(services);
    }

    /// <summary>
    /// Add Postgres CDC, deferring configuration until the service provider exists — use this when the
    /// builder needs services such as <c>IConfiguration</c> (e.g.
    /// <c>cdc.UseConnectionString(sp.GetRequiredService&lt;IConfiguration&gt;().GetConnectionString(...))</c>),
    /// which also lets test hosts redirect those values through ordinary configuration overrides.
    /// Unlike the eager overload, <paramref name="configure"/> runs (exactly once) on first resolution, so
    /// configuration errors surface at host start rather than at registration. The callback receives the
    /// <b>root</b> provider: scoped services are unavailable, and resolving Wallaby's own services
    /// (<see cref="CdcOptions"/>, <see cref="ICdcStatus"/>, …) inside it creates a resolution cycle.
    /// </summary>
    public static IServiceCollection AddWallaby(
        this IServiceCollection services, Action<IServiceProvider, CdcBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.AddSingleton(sp =>
        {
            var builder = new CdcBuilder();
            configure(sp, builder);
            return builder.Build();
        });
        return AddWallabyCore(services);
    }

    /// <summary>
    /// Registrations shared by both overloads. Every factory resolves <see cref="CdcConfiguration"/> /
    /// <see cref="CdcOptions"/> from the provider, so it works whether the configuration was registered as an
    /// instance (eager overload) or a deferred factory (provider-aware overload).
    /// </summary>
    private static IServiceCollection AddWallabyCore(IServiceCollection services)
    {
        services.AddOptions();

        // Bridge the builder's option actions into the options pipeline at THIS registration position:
        // Configure<CdcOptions> calls made before AddWallaby run first (the builder overrides them), later
        // ones override the builder, and PostConfigure always wins. Resolving CdcConfiguration lazily keeps
        // this working for both the eager (instance) and deferred (factory) registration.
        services.AddSingleton<IConfigureOptions<CdcOptions>>(sp => new ConfigureOptions<CdcOptions>(options =>
        {
            foreach (var apply in sp.GetRequiredService<CdcConfiguration>().OptionsActions)
            {
                apply(options);
            }
        }));
        services.AddSingleton<IValidateOptions<CdcOptions>>(sp =>
            new CdcOptionsValidator(sp.GetRequiredService<CdcConfiguration>()));

        // The plain CdcOptions singleton everyone injects is the pipeline's product. Validation failures are
        // rethrown as CdcConfigurationException so misconfiguration keeps its documented exception type.
        services.AddSingleton(sp =>
        {
            try
            {
                return sp.GetRequiredService<IOptions<CdcOptions>>().Value;
            }
            catch (OptionsValidationException ex)
            {
                throw new CdcConfigurationException(ex.Message, ex);
            }
        });

        services.AddSingleton(sp => new CdcDataSource(sp.GetRequiredService<CdcOptions>().ConnectionString));

        services.AddMetrics();
        services.AddSingleton(sp => new WallabyInstrumentation(sp.GetRequiredService<IMeterFactory>()));

        // Live node status surface (role, progress, faults) — read by diagnostics and health checks.
        services.AddSingleton(sp =>
        {
            var configuration = sp.GetRequiredService<CdcConfiguration>();
            return new CdcStatus(
                configuration.CaptureIntended ? sp.GetRequiredService<CdcOptions>().SlotName : "",
                TimeProvider.System);
        });
        services.AddSingleton<ICdcStatus>(sp => sp.GetRequiredService<CdcStatus>());

        services.AddSingleton<IClusterLock>(sp =>
            new Internal.Cluster.PostgresAdvisoryLock(
                sp.GetRequiredService<CdcDataSource>().Source,
                sp.GetRequiredService<CdcOptions>().LeaderHeartbeatInterval));

        // Capture runtime — registered unconditionally as lazy factories; only the hosted-service dispatch
        // below (or a consumer resolving ICdcBackfillManager) materializes it. Resolve the capture model once;
        // both the runtime and the backfill manager share this instance. The model is read via the consumer's
        // context (factory or DI scope) — no IDbContextFactory required.
        services.AddSingleton(sp =>
        {
            var config = sp.GetRequiredService<CdcConfiguration>();
            if (!config.CaptureIntended)
            {
                throw new CdcConfigurationException(
                    "This Wallaby instance is provision-only (no sinks or mappings were declared), so the " +
                    "capture/backfill runtime is unavailable. Declare a sink and a mapping to enable capture.");
            }
            var efModel = config.ModelAccessor!(sp);
            var cdc = ModelToCdcModel.Build(efModel, config.ToCaptureSpec());
            return new CapturedModel(efModel, cdc);
        });

        services.AddSingleton<ICdcBackfillManager>(sp =>
            new DefaultBackfillManager(
                sp.GetRequiredService<CapturedModel>().Cdc,
                new PostgresBackfillStore(sp.GetRequiredService<CdcDataSource>().Source)));

        services.AddSingleton<CdcRuntime>();
        services.AddSingleton<CdcBackgroundService>();
        services.AddSingleton<ExternalSlotProvisioningService>();

        // Capture: stream via the runtime. Provision-only: create the declared external slots (if any) and
        // idle — no primary slot/stream. Decided at host start, when the (possibly deferred) configuration
        // first materializes.
        services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<CdcConfiguration>().CaptureIntended
                ? (IHostedService)sp.GetRequiredService<CdcBackgroundService>()
                : sp.GetRequiredService<ExternalSlotProvisioningService>());

        return services;
    }
}
