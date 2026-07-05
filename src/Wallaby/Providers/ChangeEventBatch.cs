using Wallaby.Abstractions;

namespace Wallaby.Providers;

/// <summary>Shared helpers for provider transform invokers.</summary>
public static class ChangeEventBatch
{
    /// <summary>
    /// Cast an untyped change batch to <see cref="ChangeEvent{TEntity}"/>, validating that every change
    /// carries an entity of <typeparamref name="TEntity"/> (changes must be routed to the mapping of their
    /// own entity type).
    /// </summary>
    public static IReadOnlyList<ChangeEvent<TEntity>> Cast<TEntity>(IReadOnlyList<ChangeEvent> changes)
        where TEntity : class
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
        return typed;
    }
}
