using Wallaby.DependencyInjection;
using Wallaby.Providers;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Resolves the consumer's declared <see cref="ExternalSlotRegistration"/>s into concrete
/// <see cref="ExternalSlotSpec"/>s at startup: entity-type table declarations are resolved against the
/// storage providers' models (with a single provider directly; with several, the sole provider claiming
/// the type via <see cref="IWallabyModelProvider.Handles"/>), schema-qualified names are de-duplicated,
/// and the publication name is defaulted to <c>"{slotName}_pub"</c> when not specified.
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

            void Add(string schema, string table)
            {
                if (seen.Add((schema, table)))
                {
                    tables.Add((schema, table));
                }
            }

            foreach (var (schema, table) in registration.TableNames)
            {
                Add(schema, table);
            }

            foreach (var entityClrType in registration.EntityTypes)
            {
                var table = ResolveEntityTable(registration.SlotName, entityClrType, modelProviders);
                Add(table.Schema, table.Table);
            }

            var publication = string.IsNullOrWhiteSpace(registration.PublicationName)
                ? $"{registration.SlotName}_pub"
                : registration.PublicationName;

            specs.Add(new ExternalSlotSpec(registration.SlotName, publication, tables));
        }

        return specs;
    }

    private static QualifiedTable ResolveEntityTable(
        string slotName, Type entityClrType, IReadOnlyList<(string Name, IWallabyModelProvider Provider)> modelProviders)
    {
        if (modelProviders.Count == 0)
        {
            throw new WallabyConfigurationException(
                $"AddExternalSlot(\"{slotName}\").ForEntity<{entityClrType.Name}>() requires a " +
                "storage provider to resolve the table. Register one with UseEntityFrameworkCore<TContext>() " +
                "or use ForTable(...).");
        }

        var resolver = modelProviders.Count == 1 ? modelProviders[0].Provider : PickClaimant();

        try
        {
            return resolver.ResolveTable(entityClrType);
        }
        catch (WallabyConfigurationException ex)
        {
            throw new WallabyConfigurationException(
                $"AddExternalSlot(\"{slotName}\").ForEntity<{entityClrType.Name}>(): {ex.Message}", ex);
        }

        IWallabyModelProvider PickClaimant()
        {
            var candidates = modelProviders.Where(p => p.Provider.Handles(entityClrType)).ToList();
            return candidates.Count switch
            {
                1 => candidates[0].Provider,
                0 => throw new WallabyConfigurationException(
                    $"AddExternalSlot(\"{slotName}\").ForEntity<{entityClrType.Name}>(): no registered storage " +
                    $"provider models '{entityClrType.FullName}'. " +
                    $"Registered providers: {Describe(modelProviders.Select(p => p.Name))}."),
                _ => throw new WallabyConfigurationException(
                    $"AddExternalSlot(\"{slotName}\").ForEntity<{entityClrType.Name}>(): multiple storage providers " +
                    $"model '{entityClrType.FullName}': {Describe(candidates.Select(p => p.Name))}. " +
                    "Declare the table by name via ForTable(...)."),
            };
        }
    }

    private static string Describe(IEnumerable<string> names) => string.Join(", ", names.Select(n => $"'{n}'"));
}
