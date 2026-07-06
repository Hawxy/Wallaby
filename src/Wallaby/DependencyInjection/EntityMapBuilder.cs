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

    /// <summary>Override the document id (defaults to the source primary key).</summary>
    public EntityMapBuilder<TEntity> KeyedBy(Func<TEntity, object> keySelector)
    {
        _registration.DocumentIdSelector = change => change.Entity is TEntity entity
            ? keySelector(entity)?.ToString() ?? change.Key.ToString()
            : change.Key.ToString();
        return this;
    }

    /// <summary>Bump this when the transform/projection changes to trigger an automatic re-backfill.</summary>
    public EntityMapBuilder<TEntity> WithBackfillVersion(string version)
    {
        _registration.BackfillVersion = version;
        return this;
    }

    /// <summary>
    /// Derive a per-row scope key (e.g. tenant id) from the change. The engine sub-groups the transform
    /// batch by this key and supplies a scope-scoped enrichment session (see the provider's scoped-context
    /// registration, e.g. <c>UseScopedDbContext</c>); it also feeds <see cref="ScopedDestination"/>.
    /// </summary>
    public EntityMapBuilder<TEntity> ScopedBy(Func<TEntity, object?> keySelector)
    {
        _registration.ScopeKeySelector = change => change.Entity is TEntity entity ? keySelector(entity) : null;
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
}
