using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Hosting;
using Wallaby.Internal;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.State;
using Wallaby.Providers;

namespace Wallaby.DependencyInjection;

/// <summary>Entry point for adding Wallaby to a host.</summary>
public static class WallabyServiceCollectionExtensions
{
    /// <summary>
    /// Add Postgres CDC. Supply a connection string via <c>cdc.UseConnectionString(...)</c>. For capture
    /// (any sink) also register a storage provider,
    /// e.g. <c>cdc.UseEntityFrameworkCore&lt;TContext&gt;()</c> from the Wallaby.EntityFrameworkCore
    /// package. If only external slots are declared (no capture), Wallaby
    /// runs provision-only: it creates/reconciles those slots and never opens a primary slot or streams.
    /// Wallaby owns a pooled <c>NpgsqlDataSource</c> built from the connection string for all non-replication work.
    /// <see cref="WallabyOptions"/> participates in the standard options pipeline: <c>Configure&lt;WallabyOptions&gt;</c>
    /// and configuration binding compose with the builder's <c>ConfigureOptions</c> in registration order, and
    /// <c>PostConfigure</c> runs last; option values are validated on first resolution.
    /// </summary>
    public static IServiceCollection AddWallaby(this IServiceCollection services, Action<WallabyBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new WallabyBuilder();
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
    /// (<see cref="WallabyOptions"/>, <see cref="IWallabyStatus"/>, …) inside it creates a resolution cycle.
    /// </summary>
    public static IServiceCollection AddWallaby(
        this IServiceCollection services, Action<IServiceProvider, WallabyBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.AddSingleton(sp =>
        {
            var builder = new WallabyBuilder();
            configure(sp, builder);
            return builder.Build();
        });
        return AddWallabyCore(services);
    }

    /// <summary>
    /// Registrations shared by both overloads. Every factory resolves <see cref="WallabyConfiguration"/> /
    /// <see cref="WallabyOptions"/> from the provider, so it works whether the configuration was registered as an
    /// instance (eager overload) or a deferred factory (provider-aware overload).
    /// </summary>
    private static IServiceCollection AddWallabyCore(IServiceCollection services)
    {
        services.AddOptions();

        // Bridge the builder's option actions into the options pipeline at THIS registration position:
        // Configure<WallabyOptions> calls made before AddWallaby run first (the builder overrides them), later
        // ones override the builder, and PostConfigure always wins. Resolving WallabyConfiguration lazily keeps
        // this working for both the eager (instance) and deferred (factory) registration.
        services.AddSingleton<IConfigureOptions<WallabyOptions>>(sp => new ConfigureOptions<WallabyOptions>(options =>
        {
            foreach (var apply in sp.GetRequiredService<WallabyConfiguration>().OptionsActions)
            {
                apply(options);
            }
        }));
        services.AddSingleton<IValidateOptions<WallabyOptions>>(sp =>
            new WallabyOptionsValidator(sp.GetRequiredService<WallabyConfiguration>()));

        // The plain WallabyOptions singleton everyone injects is the pipeline's product. Validation failures are
        // rethrown as WallabyConfigurationException so misconfiguration keeps its documented exception type.
        services.AddSingleton(sp =>
        {
            try
            {
                return sp.GetRequiredService<IOptions<WallabyOptions>>().Value;
            }
            catch (OptionsValidationException ex)
            {
                throw new WallabyConfigurationException(ex.Message, ex);
            }
        });

        services.AddSingleton(sp => new WallabyDataSource(sp.GetRequiredService<WallabyOptions>().ConnectionString));

        services.AddMetrics();
        services.AddSingleton(sp => new WallabyInstrumentation(sp.GetRequiredService<IMeterFactory>()));

        // Live node status surface (role, progress, faults) — read by diagnostics and health checks.
        services.AddSingleton(sp =>
        {
            var configuration = sp.GetRequiredService<WallabyConfiguration>();
            return new WallabyStatus(
                configuration.CaptureIntended ? sp.GetRequiredService<WallabyOptions>().SlotName : "",
                TimeProvider.System);
        });
        services.AddSingleton<IWallabyStatus>(sp => sp.GetRequiredService<WallabyStatus>());

        services.AddSingleton<IClusterLock>(sp =>
            new Internal.Cluster.PostgresAdvisoryLock(
                sp.GetRequiredService<WallabyDataSource>().Source,
                sp.GetRequiredService<WallabyOptions>().LeaderHeartbeatInterval));

        // Capture runtime — registered unconditionally as lazy factories; only the hosted-service dispatch
        // below (or a consumer resolving IWallabyBackfillManager) materializes it. The providers and the
        // capture plans they build are resolved once; the runtime and the backfill manager share the merged plan.
        services.AddSingleton(sp =>
        {
            var config = sp.GetRequiredService<WallabyConfiguration>();
            if (!config.CaptureIntended)
            {
                throw new WallabyConfigurationException(
                    "This Wallaby instance is provision-only (no sinks were declared), so the " +
                    "capture/backfill runtime is unavailable. Register a sink with mapped entities to enable capture.");
            }
            return ResolvedProviderSet.Build(config, sp);
        });

        services.AddSingleton(sp => sp.GetRequiredService<ResolvedProviderSet>().MergedPlan);

        services.AddSingleton<IWallabyBackfillManager>(sp =>
            new DefaultBackfillManager(
                sp.GetRequiredService<CapturePlan>().Model,
                new PostgresBackfillStore(sp.GetRequiredService<WallabyDataSource>().Source)));

        services.AddSingleton<WallabyRuntime>();
        services.AddSingleton<WallabyBackgroundService>();
        services.AddSingleton<ExternalSlotProvisioningService>();

        // Capture: stream via the runtime. Provision-only: create the declared external slots (if any) and
        // idle — no primary slot/stream. Decided at host start, when the (possibly deferred) configuration
        // first materializes.
        services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<WallabyConfiguration>().CaptureIntended
                ? (IHostedService)sp.GetRequiredService<WallabyBackgroundService>()
                : sp.GetRequiredService<ExternalSlotProvisioningService>());

        return services;
    }
}
