namespace Wallaby.Internal;

/// <summary>Grouping that keeps first-occurrence order of keys and source order within each group.</summary>
internal static class OrderedGrouping
{
    /// <summary>
    /// Group <paramref name="source"/> by <paramref name="keySelector"/>, groups in first-occurrence order
    /// and each group's items in source order (a <c>GroupBy</c> whose ordering is guaranteed, used
    /// where commit order must survive the split).
    /// </summary>
    public static List<(TKey Key, List<T> Items)> GroupPreservingOrder<T, TKey>(
        IReadOnlyList<T> source, Func<T, TKey> keySelector)
        where TKey : notnull
        => GroupPreservingOrder(source, keySelector, static item => item);

    /// <summary>
    /// As <see cref="GroupPreservingOrder{T,TKey}"/>, storing <paramref name="itemSelector"/>'s projection
    /// of each element instead of the element itself.
    /// </summary>
    public static List<(TKey Key, List<TItem> Items)> GroupPreservingOrder<T, TKey, TItem>(
        IReadOnlyList<T> source, Func<T, TKey> keySelector, Func<T, TItem> itemSelector)
        where TKey : notnull
    {
        var byKey = new Dictionary<TKey, List<TItem>>();
        var groups = new List<(TKey, List<TItem>)>();

        foreach (var element in source)
        {
            var key = keySelector(element);
            if (!byKey.TryGetValue(key, out var group))
            {
                group = [];
                byKey[key] = group;
                groups.Add((key, group));
            }
            group.Add(itemSelector(element));
        }

        return groups;
    }
}
