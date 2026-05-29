namespace EFCore.CDC.Model;

/// <summary>
/// The set of tables selected for capture, derived from the consumer's EF Core model. Built once at
/// startup and used for self-configuration, decoding, and materialization.
/// </summary>
public sealed class CdcModel
{
    private readonly Dictionary<(string Schema, string Table), CapturedTable> _byQualifiedName;
    private readonly Dictionary<Type, CapturedTable> _byClrType;

    /// <summary>Creates a model from the given captured tables.</summary>
    public CdcModel(IReadOnlyList<CapturedTable> tables)
    {
        Tables = tables ?? throw new ArgumentNullException(nameof(tables));
        _byQualifiedName = tables.ToDictionary(t => (t.Schema, t.TableName));
        _byClrType = tables.ToDictionary(t => t.EntityClrType);
    }

    /// <summary>All captured tables.</summary>
    public IReadOnlyList<CapturedTable> Tables { get; }

    /// <summary>Find a captured table by schema and name, or null if not captured.</summary>
    public CapturedTable? FindByRelation(string schema, string table)
        => _byQualifiedName.GetValueOrDefault((schema, table));

    /// <summary>Find a captured table by mapped entity type, or null if not captured.</summary>
    public CapturedTable? FindByClrType(Type entityClrType)
        => _byClrType.GetValueOrDefault(entityClrType);
}
