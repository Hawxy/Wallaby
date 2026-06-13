using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Wallaby.DependencyInjection;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Resolves the consumer's declared <see cref="ExternalSlotRegistration"/>s into concrete
/// <see cref="ExternalSlotSpec"/>s at startup: entity-type table declarations are resolved against the EF
/// Core model, schema-qualified names are de-duplicated, and the publication name is defaulted to
/// <c>"{slotName}_pub"</c> when not specified.
/// </summary>
internal static class ExternalSlotResolver
{
    public static IReadOnlyList<ExternalSlotSpec> Resolve(
        IReadOnlyCollection<ExternalSlotRegistration> registrations, IModel? model)
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
                if (model is null)
                {
                    throw new WallabyConfigurationException(
                        $"AddExternalSlot(\"{registration.SlotName}\").ForEntity<{entityClrType.Name}>() requires a " +
                        "DbContext to resolve the table. Declare one with UseContext<TContext>() or use ForTable(...).");
                }

                var entityType = model.FindEntityType(entityClrType)
                    ?? throw new WallabyConfigurationException(
                        $"AddExternalSlot(\"{registration.SlotName}\").ForEntity<{entityClrType.Name}>(): " +
                        $"'{entityClrType.FullName}' is not part of the EF Core model.");
                var tableName = entityType.GetTableName()
                    ?? throw new WallabyConfigurationException(
                        $"AddExternalSlot(\"{registration.SlotName}\").ForEntity<{entityClrType.Name}>(): " +
                        $"'{entityClrType.FullName}' is not mapped to a table.");
                Add(entityType.GetSchema() ?? "public", tableName);
            }

            var publication = string.IsNullOrWhiteSpace(registration.PublicationName)
                ? $"{registration.SlotName}_pub"
                : registration.PublicationName;

            specs.Add(new ExternalSlotSpec(registration.SlotName, publication, tables));
        }

        return specs;
    }
}
