using Marten;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;
using Wallaby.Internal;
using Wallaby.Model;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Wallaby.Marten.Internal;

/// <summary>
/// Marten schema feature that keeps the captured tables flagged with
/// <see cref="CapturedTable.RequiresFullReplicaIdentity"/> (soft-deleted documents, scoped-destination
/// mappings) on <c>REPLICA IDENTITY FULL</c> through Marten's own migration flow. The capture plan is
/// resolved lazily — Marten only enumerates a feature's objects at migration time, after the store has
/// finished building, so this never recurses into the store's own construction.
/// </summary>
internal sealed class ReplicaIdentityFeature(IServiceProvider services, StoreOptions storeOptions) : IFeatureSchema
{
    public string Identifier => "wallaby_replica_identity";
    public Migrator Migrator => storeOptions.Advanced.Migrator;
    public Type StorageType => GetType();

    public ISchemaObject[] Objects
        => [.. FlaggedTables().Select(t => new TableReplicaIdentity(t.Schema, t.TableName))];

    /// <summary>The flagged tables' document types, so their <c>CREATE TABLE</c>s order ahead of the ALTERs.</summary>
    public IEnumerable<Type> DependentTypes() => FlaggedTables().Select(t => t.EntityClrType);

    public void WritePermissions(Migrator rules, TextWriter writer)
    {
    }

    private IReadOnlyList<CapturedTable> FlaggedTables()
    {
        var configuration = services.GetRequiredService<WallabyConfiguration>();
        if (!configuration.CaptureIntended)
        {
            return [];
        }

        var provider = services.GetRequiredService<ResolvedProviderSet>().Providers
                .FirstOrDefault(p => p.Name == MartenWallabyBuilderExtensions.ProviderName)
            ?? throw new WallabyConfigurationException(
                "ManageWallabyReplicaIdentity() requires the Marten storage provider; call UseMarten() " +
                "inside AddWallaby(...).");
        return [.. provider.Plan.Model.Tables.Where(t => t.RequiresFullReplicaIdentity)];
    }
}
