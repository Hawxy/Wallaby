using EFCore.CDC.Abstractions;
using EFCore.CDC.Internal.Pipeline;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.CDC.DependencyInjection;

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
            ? keySelector(entity)?.ToString() ?? new DocumentKey(change.PrimaryKey).ToString()
            : new DocumentKey(change.PrimaryKey).ToString();
        return this;
    }

    /// <summary>Bump this when the transform/projection changes to trigger an automatic re-backfill.</summary>
    public EntityMapBuilder<TEntity> WithBackfillVersion(string version)
    {
        _registration.BackfillVersion = version;
        return this;
    }

    /// <summary>Use a transform instance.</summary>
    public EntityMapBuilder<TEntity> UsingTransform<TDocument>(ICdcTransform<TEntity, TDocument> transform)
    {
        _registration.TransformFactory = _ => new TransformInvoker<TEntity, TDocument>(transform);
        return this;
    }

    /// <summary>Use a transform type resolved (or constructed) from the container.</summary>
    public EntityMapBuilder<TEntity> UsingTransform<TTransform, TDocument>()
        where TTransform : class, ICdcTransform<TEntity, TDocument>
    {
        _registration.TransformFactory = sp =>
            new TransformInvoker<TEntity, TDocument>(ActivatorUtilities.GetServiceOrCreateInstance<TTransform>(sp));
        return this;
    }

    /// <summary>Use an inline transform lambda (the trivial, no-class case).</summary>
    public EntityMapBuilder<TEntity> UsingTransform<TDocument>(
        Func<DbContext, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, TDocument?>>> handler)
    {
        _registration.TransformFactory = _ =>
            new TransformInvoker<TEntity, TDocument>(new DelegateTransform<TEntity, TDocument>(handler));
        return this;
    }
}
