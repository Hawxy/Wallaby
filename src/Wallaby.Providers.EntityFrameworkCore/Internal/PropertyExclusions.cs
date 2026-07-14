using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Wallaby.Providers.EntityFrameworkCore.Internal;

/// <summary>
/// Properties dropped from capture via <see cref="EfCoreProviderOptions.ExcludeProperty{TEntity}"/>,
/// keyed by entity CLR type. Validated once against the <see cref="IModel"/> when the capture plan is built.
/// </summary>
internal sealed class PropertyExclusions
{
    public static readonly PropertyExclusions None = new();

    private readonly Dictionary<Type, HashSet<string>> _byType = [];

    public void Add(Type entityClrType, string propertyName)
    {
        if (!_byType.TryGetValue(entityClrType, out var names))
        {
            _byType[entityClrType] = names = [];
        }
        names.Add(propertyName);
    }

    public bool IsExcluded(Type entityClrType, string propertyName)
        => _byType.TryGetValue(entityClrType, out var names) && names.Contains(propertyName);

    public void Validate(IModel model)
    {
        foreach (var (clrType, names) in _byType)
        {
            var entityType = model.FindEntityType(clrType)
                ?? throw new WallabyConfigurationException(
                    $"ExcludeProperty<{clrType.Name}>(...): the entity is not part of the DbContext model.");

            foreach (var name in names)
            {
                var property = entityType.FindProperty(name)
                    ?? throw new WallabyConfigurationException(
                        $"ExcludeProperty<{clrType.Name}>(e => e.{name}): '{name}' is not a mapped scalar " +
                        "property of the entity (navigations cannot be excluded).");
                if (property.IsPrimaryKey())
                {
                    throw new WallabyConfigurationException(
                        $"ExcludeProperty<{clrType.Name}>(e => e.{name}): a primary-key property cannot be excluded.");
                }
            }
        }
    }
}
