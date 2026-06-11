using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Non-generic adapter that lets the router invoke a strongly-typed <see cref="ICdcTransform{TEntity}"/>
/// over a batch of change events without knowing the entity type.
/// </summary>
internal interface ITransformInvoker
{
    Task<IReadOnlyDictionary<DocumentKey, CdcDocument?>> InvokeAsync(
        DbContext db, IReadOnlyList<ChangeEvent> changes, CancellationToken ct);
}

/// <summary>Wraps an <see cref="ICdcTransform{TEntity}"/> as an <see cref="ITransformInvoker"/>.</summary>
internal sealed class TransformInvoker<TEntity>(ICdcTransform<TEntity> transform)
    : ITransformInvoker
    where TEntity : class
{
    public Task<IReadOnlyDictionary<DocumentKey, CdcDocument?>> InvokeAsync(
        DbContext db, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
    {
        var typed = new List<ChangeEvent<TEntity>>(changes.Count);
        foreach (var change in changes)
        {
            if (change.Entity is not null && change.Entity is not TEntity)
            {
                throw new InvalidOperationException(
                    $"Cannot invoke the transform for '{typeof(TEntity).Name}': the change for " +
                    $"'{change.Metadata.QualifiedTableName}' carries an entity of type " +
                    $"'{change.Entity.GetType().Name}'. Changes must be routed to the mapping of their " +
                    "own entity type.");
            }

            typed.Add(new ChangeEvent<TEntity>(
                change.Action, change.Metadata, (TEntity?)change.Entity,
                change.Record, change.Changes, change.PrimaryKey)
            {
                Key = change.Key,
            });
        }
        return transform.TransformAsync(db, typed, ct);
    }
}
