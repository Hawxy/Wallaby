using Wallaby.Aspire.Hosting.Internal;

namespace Aspire.Hosting.ApplicationModel;

/// <summary>
/// A dashboard view of a Wallaby installation: polls its control plane (slots, backfills, suspension)
/// through the database and exposes commands to backfill, suspend, resume and widen publications. The
/// capture pipeline itself runs in <see cref="Host"/>; this resource never starts a process.
/// </summary>
public sealed class WallabyResource : Resource, IResourceWithWaitSupport
{
    /// <summary>Create the resource.</summary>
    /// <param name="name">The resource name.</param>
    /// <param name="database">The database Wallaby captures from, where its <c>wallaby</c> schema lives.</param>
    /// <param name="host">The resource running Wallaby (usually the CDC worker project).</param>
    public WallabyResource(string name, IResourceWithConnectionString database, IResource host)
        : base(name)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(host);
        Database = database;
        Host = host;
    }

    /// <summary>The database Wallaby captures from, where its <c>wallaby</c> schema lives.</summary>
    public IResourceWithConnectionString Database { get; }

    /// <summary>The resource running Wallaby (usually the CDC worker project).</summary>
    public IResource Host { get; }

    /// <summary>How often the control plane is polled. Defaults to 2 seconds.</summary>
    public TimeSpan PollInterval { get; internal set; } = TimeSpan.FromSeconds(2);

    internal string HealthCheckKey => $"{Name}_wallaby";

    internal WallabyMonitorState Monitor { get; } = new();
}
