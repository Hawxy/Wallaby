using EFCore.CDC.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace EFCore.CDC.Internal.Pipeline;

/// <summary>
/// Non-generic adapter that lets the router invoke a strongly-typed <see cref="ICdcTransform{TEntity,TDocument}"/>
/// over a batch of change events without knowing the generic types.
/// </summary>
internal interface ITransformInvoker
{
    Task<IReadOnlyDictionary<DocumentKey, object?>> InvokeAsync(
        DbContext db, IReadOnlyList<ChangeEvent> changes, CancellationToken ct);
}

/// <summary>Wraps an <see cref="ICdcTransform{TEntity,TDocument}"/> as an <see cref="ITransformInvoker"/>.</summary>
internal sealed class TransformInvoker<TEntity, TDocument>(ICdcTransform<TEntity, TDocument> transform)
    : ITransformInvoker
    where TEntity : class
{
    public async Task<IReadOnlyDictionary<DocumentKey, object?>> InvokeAsync(
        DbContext db, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
    {
        var typed = new List<ChangeEvent<TEntity>>(changes.Count);
        foreach (var change in changes)
        {
            typed.Add(new ChangeEvent<TEntity>(
                change.Action, change.Metadata, change.Entity as TEntity,
                change.Record, change.Changes, change.PrimaryKey));
        }

        var documents = await transform.TransformAsync(db, typed, ct);

        var boxed = new Dictionary<DocumentKey, object?>(documents.Count);
        foreach (var (key, document) in documents)
        {
            boxed[key] = document;
        }
        return boxed;
    }
}
