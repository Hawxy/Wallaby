using Wallaby.DependencyInjection;
using Wallaby.Providers;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Resolves the consumer's declared <see cref="ExternalSlotRegistration"/>s into concrete
/// <see cref="ExternalSlotSpec"/>s at startup: <c>ForAllEntities()</c> expands to every table of every
/// storage provider's model, entity-type declarations and exclusions are resolved against those models
/// (with a single provider directly; with several, the sole provider claiming the type via
/// <see cref="IWallabyModelProvider.Handles"/>), schema-qualified names are de-duplicated, exclusions are
/// subtracted, and the publication name is defaulted to <c>"{slotName}_pub"</c> when not specified.
/// </summary>
internal static class ExternalSlotResolver
{
    public static IReadOnlyList<ExternalSlotSpec> Resolve(
        IReadOnlyCollection<ExternalSlotRegistration> registrations,
        IReadOnlyList<(string Name, IWallabyModelProvider Provider)> modelProviders)
    {
        if (registrations.Count == 0)
        {
            return [];
        }

        var specs = new List<ExternalSlotSpec>(registrations.Count);
        foreach (var registration in registrations)
        {
            var tables = new List<(string Schema, string Table)>();
            var seen = new HashSet<(string, string)>();
            var declared = new HashSet<(string, string)>();

            void Add(string schema, string table)
            {
                if (seen.Add((schema, table)))
                {
                    tables.Add((schema, table));
                }
            }

            if (registration.AllEntities)
            {
                if (modelProviders.Count == 0)
                {
                    throw new WallabyConfigurationException(
                        $"AddExternalSlot(\"{registration.SlotName}\").ForAllEntities() requires a storage provider " +
                        "to resolve the model. Register one with UseEntityFrameworkCore<TContext>(), UseMarten() " +
                        "or UseTables(...), or declare the tables by name via ForTable(...).");
                }
                foreach (var (_, provider) in modelProviders)
                {
                    foreach (var table in provider.ResolveAllTables())
                    {
                        Add(table.Schema, table.Table);
                    }
                }
            }

            foreach (var (schema, table) in registration.TableNames)
            {
                Add(schema, table);
                declared.Add((schema, table));
            }

            foreach (var entityClrType in registration.EntityTypes)
            {
                var table = ResolveEntityTable(registration.SlotName, entityClrType, modelProviders, ForEntity);
                Add(table.Schema, table.Table);
                declared.Add((table.Schema, table.Table));
            }

            var exclusions = registration.ExcludedTableNames
                .Concat(registration.ExcludedEntityTypes.Select(type =>
                {
                    var table = ResolveEntityTable(registration.SlotName, type, modelProviders, Except);
                    return (table.Schema, table.Table);
                }));
            foreach (var (schema, table) in exclusions)
            {
                if (declared.Contains((schema, table)))
                {
                    throw new WallabyConfigurationException(
                        $"AddExternalSlot(\"{registration.SlotName}\").Except(...) excludes '{schema}.{table}', which the " +
                        "slot also declares via ForTable(...) or ForEntity<T>().");
                }
                if (!seen.Remove((schema, table)))
                {
                    var elsewhere = tables.Where(t => t.Table == table).Select(t => $"'{t.Schema}.{t.Table}'").ToList();
                    var hint = elsewhere.Count > 0 ? $" Did you mean {string.Join(" or ", elsewhere)}?" : "";
                    throw new WallabyConfigurationException(
                        $"AddExternalSlot(\"{registration.SlotName}\").Except(...) excludes '{schema}.{table}', which no " +
                        $"registered storage provider models (names are case-sensitive).{hint}");
                }
                tables.Remove((schema, table));
            }

            if (tables.Count == 0)
            {
                throw new WallabyConfigurationException(
                    $"AddExternalSlot(\"{registration.SlotName}\") resolves to no tables after exclusions " +
                    $"(providers: {Describe(modelProviders.Select(p => p.Name))}).");
            }

            var publication = string.IsNullOrWhiteSpace(registration.PublicationName)
                ? $"{registration.SlotName}_pub"
                : registration.PublicationName;

            specs.Add(new ExternalSlotSpec(registration.SlotName, publication, tables));
        }

        return specs;
    }

    // The builder member being resolved and the by-name alternative its errors should point at.
    private static readonly (string Member, string ByName) ForEntity = ("ForEntity", "ForTable(...)");
    private static readonly (string Member, string ByName) Except = ("Except", "Except(schema, table)");

    private static QualifiedTable ResolveEntityTable(
        string slotName, Type entityClrType, IReadOnlyList<(string Name, IWallabyModelProvider Provider)> modelProviders,
        (string Member, string ByName) via)
    {
        var call = $"AddExternalSlot(\"{slotName}\").{via.Member}<{entityClrType.Name}>()";
        if (modelProviders.Count == 0)
        {
            throw new WallabyConfigurationException(
                $"{call} requires a storage provider to resolve the table. Register one with " +
                $"UseEntityFrameworkCore<TContext>(), UseMarten() or UseTables(...), or use {via.ByName}.");
        }

        var resolver = modelProviders.Count == 1 ? modelProviders[0].Provider : PickClaimant();

        try
        {
            return resolver.ResolveTable(entityClrType);
        }
        catch (WallabyConfigurationException ex)
        {
            throw new WallabyConfigurationException($"{call}: {ex.Message}", ex);
        }

        IWallabyModelProvider PickClaimant()
        {
            var candidates = modelProviders.Where(p => p.Provider.Handles(entityClrType)).ToList();
            return candidates.Count switch
            {
                1 => candidates[0].Provider,
                0 => throw new WallabyConfigurationException(
                    $"{call}: no registered storage provider models '{entityClrType.FullName}'. " +
                    $"Registered providers: {Describe(modelProviders.Select(p => p.Name))}."),
                _ => throw new WallabyConfigurationException(
                    $"{call}: multiple storage providers model '{entityClrType.FullName}': " +
                    $"{Describe(candidates.Select(p => p.Name))}. Declare the table by name via {via.ByName}."),
            };
        }
    }

    private static string Describe(IEnumerable<string> names) => string.Join(", ", names.Select(n => $"'{n}'"));
}
