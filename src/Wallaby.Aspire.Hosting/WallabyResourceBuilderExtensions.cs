using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Aspire.Hosting.Internal;

namespace Aspire.Hosting;

/// <summary>Aspire AppHost extensions for running and operating Wallaby.</summary>
public static class WallabyResourceBuilderExtensions
{
    /// <summary>
    /// Start the Postgres container with <c>wal_level=logical</c> and room for replication slots, which
    /// Wallaby's logical replication requires.
    /// </summary>
    /// <param name="builder">The Postgres server resource.</param>
    /// <param name="maxReplicationSlots">The server's <c>max_replication_slots</c> and <c>max_wal_senders</c>.</param>
    public static IResourceBuilder<PostgresServerResource> WithLogicalReplication(
        this IResourceBuilder<PostgresServerResource> builder, int maxReplicationSlots = 10)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxReplicationSlots, 1);
        return builder.WithArgs(
            "-c", "wal_level=logical",
            "-c", $"max_replication_slots={maxReplicationSlots}",
            "-c", $"max_wal_senders={maxReplicationSlots}");
    }

    /// <summary>
    /// Show the Wallaby installation that <paramref name="builder"/> runs in the dashboard: a child
    /// resource (named <c>{resource}-wallaby</c> by default) with its replication slots, backfill progress
    /// and suspension state, and commands to backfill, suspend, resume and widen publications. It is
    /// healthy once every slot streams and no backfill is pending; wait on it with
    /// <see cref="WaitForWallaby{T}"/>.
    /// </summary>
    /// <param name="builder">The resource running Wallaby, usually the CDC worker project.</param>
    /// <param name="database">The database Wallaby captures from, where its <c>wallaby</c> schema lives.</param>
    /// <param name="configure">Further configuration of the Wallaby resource, e.g. <see cref="WithPollInterval"/>.</param>
    /// <param name="name">The Wallaby resource's name; defaults to <c>{resource}-wallaby</c>.</param>
    public static IResourceBuilder<T> WithWallaby<T>(
        this IResourceBuilder<T> builder,
        IResourceBuilder<IResourceWithConnectionString> database,
        Action<IResourceBuilder<WallabyResource>>? configure = null,
        string? name = null)
        where T : IResource
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(database);

        var app = builder.ApplicationBuilder;
        var resource = new WallabyResource(name ?? $"{builder.Resource.Name}-wallaby", database.Resource, builder.Resource);
        app.Services.AddHealthChecks().AddCheck(resource.HealthCheckKey, new WallabyResourceHealthCheck(resource));

        // No initial snapshot: Aspire seeds the parent relationship only into a snapshot it creates itself.
        var wallaby = app.AddResource(resource)
            .WithIconName("ArrowSync")
            .WithParentRelationship(builder.Resource)
            .WaitFor(database)
            .WithHealthCheck(resource.HealthCheckKey)
            .ExcludeFromManifest()
            .OnInitializeResource(WallabyMonitor.StartAsync);
        WallabyCommands.Add(wallaby);

        configure?.Invoke(wallaby);
        return builder;
    }

    /// <summary>How often the dashboard polls Wallaby's control plane. Defaults to 2 seconds.</summary>
    /// <param name="builder">The Wallaby resource.</param>
    /// <param name="interval">The poll interval.</param>
    public static IResourceBuilder<WallabyResource> WithPollInterval(
        this IResourceBuilder<WallabyResource> builder, TimeSpan interval)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        builder.Resource.PollInterval = interval;
        return builder;
    }

    /// <summary>
    /// Start <paramref name="builder"/> only once the Wallaby installation run by <paramref name="host"/>
    /// is caught up: its slots stream and no backfill is pending.
    /// </summary>
    /// <param name="builder">The resource to delay.</param>
    /// <param name="host">The resource <see cref="WithWallaby{T}"/> was called on.</param>
    /// <exception cref="InvalidOperationException"><paramref name="host"/> has no Wallaby resource.</exception>
    public static IResourceBuilder<T> WaitForWallaby<T>(this IResourceBuilder<T> builder, IResourceBuilder<IResource> host)
        where T : IResourceWithWaitSupport
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(host);

        var wallabies = builder.ApplicationBuilder.Resources
            .OfType<WallabyResource>()
            .Where(r => ReferenceEquals(r.Host, host.Resource))
            .ToList();
        if (wallabies.Count == 0)
        {
            throw new InvalidOperationException(
                $"Resource '{host.Resource.Name}' has no Wallaby resource; call WithWallaby on it before WaitForWallaby.");
        }
        foreach (var wallaby in wallabies)
        {
            builder.WaitFor(builder.ApplicationBuilder.CreateResourceBuilder(wallaby));
        }
        return builder;
    }
}
