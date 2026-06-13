using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;

namespace Wallaby.DependencyInjection;

/// <summary>
/// Configures the routing for one entity type: which sink/destination it goes to, its document-id rule,
/// its backfill version, and the transform that shapes its document. The transform holds all the
/// enrichment/transformation logic; everything here is routing.
/// </summary>
public sealed class EntityMapBuilder<TEntity> where TEntity : class
{
    private readonly MappingRegistration _registration;

    internal EntityMapBuilder(MappingRegistration registration) => _registration = registration;

    /// <summary>Route this entity's documents to the named sink and (optional) destination/index.</summary>
    public EntityMapBuilder<TEntity> ToSink(string sinkName, string? destination = null)
    {
        _registration.SinkName = sinkName;
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
    /// batch by this key and supplies a scope-scoped enrichment <c>DbContext</c> (see <c>UseScopedContext</c>);
    /// it also feeds <see cref="ScopedDestination"/>.
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

    /// <summary>Use a transform instance.</summary>
    public EntityMapBuilder<TEntity> UsingTransform(IWallabyTransform<TEntity> transform)
    {
        _registration.TransformFactory = _ => new TransformInvoker<TEntity>(transform);
        return this;
    }

    /// <summary>Use a transform type resolved (or constructed) from the container.</summary>
    public EntityMapBuilder<TEntity> UsingTransform<TTransform>()
        where TTransform : class, IWallabyTransform<TEntity>
    {
        _registration.TransformFactory = sp =>
            new TransformInvoker<TEntity>(ActivatorUtilities.GetServiceOrCreateInstance<TTransform>(sp));
        return this;
    }

    /// <summary>Use an inline transform lambda (the trivial, no-class case).</summary>
    public EntityMapBuilder<TEntity> UsingTransform(
        Func<DbContext, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> handler)
    {
        _registration.TransformFactory = _ =>
            new TransformInvoker<TEntity>(new DelegateTransform<TEntity>(handler));
        return this;
    }

    /// <summary>
    /// Declare that changes to the table behind <paramref name="navigation"/> should fan out and re-emit
    /// this entity. Use this when the transform reads data from related tables (a referenced principal,
    /// a many-to-many skip-navigation's join table, or an owned side table) — otherwise those changes
    /// would not reach the pipeline. The navigation expression is resolved against the EF Core model at
    /// startup; it must point at a single one-hop navigation (no chains, no method calls).
    /// </summary>
    public EntityMapBuilder<TEntity> DependsOn<TNav>(Expression<Func<TEntity, TNav>> navigation)
    {
        ArgumentNullException.ThrowIfNull(navigation);
        _registration.DeclaredDependencies.Add(navigation);
        return this;
    }
}
