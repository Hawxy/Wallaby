namespace Wallaby.Model;

/// <summary>Lookups over a decoded tuple (<see cref="RawChange.NewValues"/> / <see cref="RawChange.OldValues"/>).</summary>
public static class RawColumnExtensions
{
    /// <summary>The column with this name, or null when the tuple does not carry it.</summary>
    public static RawColumn? Find(this IReadOnlyList<RawColumn> columns, string columnName)
    {
        for (var i = 0; i < columns.Count; i++)
        {
            if (columns[i].ColumnName == columnName)
            {
                return columns[i];
            }
        }
        return null;
    }
}
