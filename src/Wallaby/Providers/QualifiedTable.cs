namespace Wallaby.Providers;

/// <summary>A schema-qualified Postgres table name.</summary>
/// <param name="Schema">The schema name (e.g. <c>public</c>).</param>
/// <param name="Table">The table name.</param>
public readonly record struct QualifiedTable(string Schema, string Table);
