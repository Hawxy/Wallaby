namespace EFCore.CDC.Model;

/// <summary>
/// The set of tables selected for capture, derived from the consumer's EF Core model. Built once at
/// startup and used for self-configuration, decoding, and materialization. Includes both the directly
/// mapped (primary) tables and any tables captured only to drive dependent fan-out.
/// </summary>
public sealed class CdcModel
{
    private readonly Dictionary<(string Schema, string Table), CapturedTable> _byQualifiedName;
    private readonly Dictionary<Type, CapturedTable> _byClrType;
    private readonly Dictionary<(string Schema, string Table), List<DependentBinding>> _bindingsByDependentTable;

    public CdcModel(IReadOnlyList<CapturedTable> tables)
        : this(tables, Array.Empty<DependentBinding>())
    {
    }

    public CdcModel(IReadOnlyList<CapturedTable> tables, IReadOnlyList<DependentBinding> dependentBindings)
    {
        Tables = tables ?? throw new ArgumentNullException(nameof(tables));
        DependentBindings = dependentBindings ?? throw new ArgumentNullException(nameof(dependentBindings));

        _byQualifiedName = tables.ToDictionary(t => (t.Schema, t.TableName));
        // A CLR type may appear once (typical) or be shared across primary/dependent — last write wins.
        _byClrType = tables.GroupBy(t => t.EntityClrType).ToDictionary(g => g.Key, g => g.First());

        _bindingsByDependentTable = new Dictionary<(string, string), List<DependentBinding>>();
        foreach (var binding in dependentBindings)
        {
            var key = (binding.DependentTable.Schema, binding.DependentTable.TableName);
            if (!_bindingsByDependentTable.TryGetValue(key, out var list))
            {
                list = [];
                _bindingsByDependentTable[key] = list;
            }
            list.Add(binding);
        }
    }

    /// <summary>All captured tables (primary mappings plus any tables captured only for fan-out).</summary>
    public IReadOnlyList<CapturedTable> Tables { get; }

    /// <summary>Rules that fan a dependent-table change out to synthetic primary-table updates.</summary>
    public IReadOnlyList<DependentBinding> DependentBindings { get; }

    /// <summary>Find a captured table by schema and name, or null if not captured.</summary>
    public CapturedTable? FindByRelation(string schema, string table)
        => _byQualifiedName.GetValueOrDefault((schema, table));

    /// <summary>Find a captured table by mapped entity type, or null if not captured.</summary>
    public CapturedTable? FindByClrType(Type entityClrType)
        => _byClrType.GetValueOrDefault(entityClrType);

    /// <summary>Bindings whose dependent table matches; empty when the table is not a fan-out source.</summary>
    public IReadOnlyList<DependentBinding> FindBindingsForDependent(string schema, string table)
        => _bindingsByDependentTable.TryGetValue((schema, table), out var list)
            ? list
            : Array.Empty<DependentBinding>();
}
