using Wallaby.DependencyInjection;
using Wallaby.Providers;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Resolves the consumer's declared <see cref="ExternalSlotRegistration"/>s into concrete
/// <see cref="ExternalSlotSpec"/>s at startup: entity-type table declarations are resolved against the
/// storage provider's model, schema-qualified names are de-duplicated, and the publication name is
/// defaulted to <c>"{slotName}_pub"</c> when not specified.
/// </summary>
internal static class ExternalSlotResolver
{
    public static IReadOnlyList<ExternalSlotSpec> Resolve(
        IReadOnlyCollection<ExternalSlotRegistration> registrations, IWallabyModelProvider? modelProvider)
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
                if (modelProvider is null)
                {
                    throw new WallabyConfigurationException(
                        $"AddExternalSlot(\"{registration.SlotName}\").ForEntity<{entityClrType.Name}>() requires a " +
                        "storage provider to resolve the table. Register one with UseEntityFrameworkCore<TContext>() " +
                        "or use ForTable(...).");
                }

                QualifiedTable table;
                try
                {
                    table = modelProvider.ResolveTable(entityClrType);
                }
                catch (WallabyConfigurationException ex)
                {
                    throw new WallabyConfigurationException(
                        $"AddExternalSlot(\"{registration.SlotName}\").ForEntity<{entityClrType.Name}>(): {ex.Message}", ex);
                }
                Add(table.Schema, table.Table);
            }

            var publication = string.IsNullOrWhiteSpace(registration.PublicationName)
                ? $"{registration.SlotName}_pub"
                : registration.PublicationName;

            specs.Add(new ExternalSlotSpec(registration.SlotName, publication, tables));
        }

        return specs;
    }
}
