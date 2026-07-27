using System.ComponentModel;
using System.Linq.Expressions;
using Wallaby.Abstractions;
using Wallaby.Providers;

namespace Wallaby.DependencyInjection;

/// <summary>
/// Configures one entity mapping of a sink: its destination, document-id rule, backfill version, and the
/// transform that shapes its document. The transform holds all the enrichment/transformation logic;
/// everything here is routing.
/// </summary>
public sealed class EntityMapBuilder<TEntity> where TEntity : class
{
    private readonly MappingRegistration _registration;

    internal EntityMapBuilder(MappingRegistration registration) => _registration = registration;

    /// <summary>
    /// Route this entity's documents to a destination within the sink (e.g. an index or topic). When
    /// omitted, the sink's default destination applies.
    /// </summary>
    public EntityMapBuilder<TEntity> ToDestination(string destination)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        _registration.Destination = destination;
        return this;
    }

    /// <summary>
    /// Override the document id (defaults to the source primary key). Because deletes must also compute
    /// the id, the table requires <c>REPLICA IDENTITY FULL</c> so the full old row is present on delete;
    /// self-configuration fails at startup when it is missing.
    /// </summary>
    public EntityMapBuilder<TEntity> KeyedBy(Func<TEntity, object> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _registration.HasEntityKeyedId = true;
        _registration.DocumentIdSelector = change =>
        {
            if (change.Entity is not TEntity entity)
            {
                // A primary-key fallback would target a document that was never written, so the orphan
                // under the custom id lingers forever.
                throw new InvalidOperationException(
                    $"KeyedBy for '{typeof(TEntity).Name}' could not compute the document id: the " +
                    $"{change.Action} on '{change.Metadata.QualifiedTableName}' (primary key {change.Key}) " +
                    "carried no materialized entity. Ensure the table has REPLICA IDENTITY FULL so deletes " +
                    "carry the full old row (self-config logs the exact DDL), or drop KeyedBy to key by " +
                    "primary key.");
            }
            return keySelector(entity)?.ToString()
                ?? throw new InvalidOperationException(
                    $"KeyedBy selector for '{typeof(TEntity).Name}' returned null on {change.Action} of " +
                    $"'{change.Metadata.QualifiedTableName}' (primary key {change.Key}). The key columns are " +
                    "likely missing from the replicated old row; run: " +
                    $"ALTER TABLE {change.Metadata.QualifiedTableName} REPLICA IDENTITY FULL;");
        };
        return this;
    }

    /// <summary>Bump this when the transform/projection changes to trigger an automatic re-backfill.</summary>
    /// <param name="version">The declared transform/projection version.</param>
    /// <param name="purgeOnChange">
    /// Purge sink destinations before the re-backfill a version change triggers, so documents whose ids
    /// or shape changed don't linger under old keys. Backfill is per table, so every non-scoped
    /// (sink, destination) pair mapped to this entity's table is purged — including other mappings'.
    /// Requires sinks to implement <see cref="Abstractions.ISinkPurger"/>.
    /// </param>
    public EntityMapBuilder<TEntity> WithBackfillVersion(string version, bool purgeOnChange = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(version);
        _registration.BackfillVersion = version;
        _registration.PurgeOnBackfillVersionChange = purgeOnChange;
        return this;
    }

    /// <summary>
    /// Derive a per-row scope key (e.g. tenant id) from the change. The engine sub-groups the transform
    /// batch by this key and supplies a scope-scoped enrichment session (see the provider's scoped-context
    /// registration, e.g. <c>UseScopedDbContext</c>); it also feeds <see cref="ScopedDestination"/>.
    /// Combined with <see cref="ScopedDestination"/>, the table requires <c>REPLICA IDENTITY FULL</c>
    /// (deletes must resolve their destination from the entity); the <see cref="ChangeEvent"/> overload
    /// reads captured columns instead and carries no such requirement.
    /// </summary>
    public EntityMapBuilder<TEntity> ScopedBy(Func<TEntity, object?> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        var registration = _registration;
        registration.HasEntityScopedKey = true;
        registration.ScopeKeySelector = change =>
        {
            if (change.Entity is TEntity entity)
            {
                return keySelector(entity);
            }
            if (registration.DestinationSelector is not null)
            {
                // A null key would resolve the ScopedDestination to the wrong index.
                throw new InvalidOperationException(
                    $"ScopedBy for '{typeof(TEntity).Name}' could not compute the scope key: the " +
                    $"{change.Action} on '{change.Metadata.QualifiedTableName}' (primary key {change.Key}) " +
                    "carried no materialized entity, so its ScopedDestination cannot be resolved. Ensure " +
                    "the table has REPLICA IDENTITY FULL (self-config logs the exact DDL), or use the " +
                    "ChangeEvent overload of ScopedBy to read the key from a captured column.");
            }
            return null; // enrichment-only scoping: the key is unused for deletes
        };
        return this;
    }

    /// <summary>
    /// Derive a per-row scope key from the raw <see cref="ChangeEvent"/>. Use this overload when the key is not
    /// a property of the entity itself but lives in another captured column, for example a shadow property such
    /// as a multi-tenancy <c>tenant_id</c> (read it via <c>c.Record["TenantId"]</c>). 
    /// </summary>
    public EntityMapBuilder<TEntity> ScopedBy(Func<ChangeEvent, object?> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _registration.ScopeKeySelector = keySelector;
        return this;
    }

    /// <summary>
    /// Route documents to a destination computed from the scope key (e.g. an index per tenant). Requires
    /// a <c>ScopedBy(...)</c>; because deletes must also resolve the destination, the table is marked to need
    /// <c>REPLICA IDENTITY FULL</c> so the scope key is present on delete.
    /// </summary>
    public EntityMapBuilder<TEntity> ScopedDestination(Func<object?, string?> destinationByScopeKey)
    {
        ArgumentNullException.ThrowIfNull(destinationByScopeKey);
        _registration.DestinationSelector = destinationByScopeKey;
        return this;
    }

    /// <summary>
    /// Register the transform via a provider-built invoker. Provider packages call this from their typed
    /// <c>UsingTransform</c> extensions (e.g. Wallaby.Providers.EntityFrameworkCore's DbContext-typed overloads);
    /// use those instead of calling this directly. A provider-typed extension passes its
    /// <paramref name="providerName"/> so the mapping resolves to the provider whose session type the
    /// transform expects.
    /// </summary>
    public EntityMapBuilder<TEntity> UsingTransformInvoker(
        Func<IServiceProvider, IWallabyTransformInvoker> factory, string? providerName = null)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _registration.TransformFactory = factory;
        _registration.TransformProviderName = providerName;
        return this;
    }

    /// <summary>
    /// Pin this mapping to the named storage provider. Only needed when more than one registered provider
    /// models <typeparamref name="TEntity"/> and the transform's type doesn't already decide it — the usual
    /// auto-resolution assigns each mapping to the sole provider that models its type.
    /// </summary>
    public EntityMapBuilder<TEntity> FromProvider(string providerName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        _registration.ExplicitProviderName = providerName;
        return this;
    }

    /// <summary>
    /// Declare a dependent navigation via a raw lambda. Provider packages call this from their typed
    /// <c>DependsOn</c> extensions (e.g. Wallaby.Providers.EntityFrameworkCore's); use those instead of calling
    /// this directly. The expression is resolved against the storage provider's model at startup.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public EntityMapBuilder<TEntity> DependsOnNavigation(LambdaExpression navigation)
    {
        ArgumentNullException.ThrowIfNull(navigation);
        _registration.DeclaredDependencies.Add(navigation);
        return this;
    }

    /// <summary>
    /// Declare the properties this mapping's transform consumes via a raw selection. Provider packages
    /// call this from their typed extensions (e.g. Wallaby.Providers.EntityFrameworkCore's <c>Consumes</c>/
    /// <c>ConsumesAllExcept</c>); use those instead of calling this directly. Repeated same-mode calls
    /// accumulate; mixing modes on one mapping fails.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public EntityMapBuilder<TEntity> SelectColumns(ColumnSelectionMode mode, IReadOnlyList<string> propertyNames)
    {
        ArgumentNullException.ThrowIfNull(propertyNames);
        if (propertyNames.Count == 0)
        {
            throw new WallabyConfigurationException(
                $"A column selection for '{typeof(TEntity).Name}' must name at least one property.");
        }

        if (_registration.ColumnSelection is { } existing)
        {
            if (existing.Mode != mode)
            {
                throw new WallabyConfigurationException(
                    $"The mapping for '{typeof(TEntity).Name}' already declares a column selection in " +
                    $"{existing.Mode} mode; Consumes(...) and ConsumesAllExcept(...) cannot be combined on one mapping.");
            }
            _registration.ColumnSelection = existing with
            {
                PropertyNames = [.. existing.PropertyNames, .. propertyNames],
            };
        }
        else
        {
            _registration.ColumnSelection = new ColumnSelection(mode, propertyNames);
        }
        return this;
    }
}
