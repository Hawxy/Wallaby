using Marten;
using Wallaby.Providers.Marten.Internal;
using static Marten.MartenServiceCollectionExtensions;

namespace Wallaby.Providers.Marten;

/// <summary>Wallaby opt-ins for Marten's fluent registration chain.</summary>
public static class MartenWallabyConfigurationExtensions
{
    /// <summary>
    /// Let Marten's schema management own the <c>REPLICA IDENTITY FULL</c> DDL for captured tables that
    /// need it (soft-deleted documents, mappings with a scoped destination). The statements join Marten's
    /// normal migration flow — applied by <c>ApplyAllDatabaseChangesOnStartup()</c> /
    /// <c>ApplyAllConfiguredChangesToDatabaseAsync()</c> and included in exported patches — and a table
    /// already on full replica identity produces no statement. The affected tables are derived from the
    /// Wallaby capture model at migration time.
    /// </summary>
    public static MartenConfigurationExpression ManageWallabyReplicaIdentity(
        this MartenConfigurationExpression marten)
    {
        ArgumentNullException.ThrowIfNull(marten);
        marten.Services.ConfigureMarten(
            (sp, storeOptions) => storeOptions.Storage.Add(new ReplicaIdentityFeature(sp, storeOptions)));
        return marten;
    }

    /// <summary>
    /// <inheritdoc cref="ManageWallabyReplicaIdentity(MartenConfigurationExpression)" path="/summary"/>
    /// </summary>
    public static MartenStoreExpression<TStore> ManageWallabyReplicaIdentity<TStore>(
        this MartenStoreExpression<TStore> marten)
        where TStore : class, IDocumentStore
    {
        ArgumentNullException.ThrowIfNull(marten);
        marten.Services.ConfigureMarten<TStore>(
            (sp, storeOptions) => storeOptions.Storage.Add(new ReplicaIdentityFeature(sp, storeOptions)));
        return marten;
    }
}
