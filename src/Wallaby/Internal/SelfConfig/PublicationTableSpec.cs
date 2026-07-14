namespace Wallaby.Internal.SelfConfig;

/// <summary>A table the publication should contain. <see cref="Columns"/> == null publishes the whole table.</summary>
internal sealed record PublicationTableSpec(
    string Schema,
    string Table,
    IReadOnlyList<string>? Columns)
{
    public static PublicationTableSpec WholeTable(string schema, string table) => new(schema, table, null);

    public string QualifiedName => $"{Schema}.{Table}";
}
