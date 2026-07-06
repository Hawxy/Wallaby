using Wallaby.DependencyInjection;
using Wallaby.Providers;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Assigns each mapped entity type to a storage provider at startup. With one registered provider
/// everything resolves to it directly (its own model errors surface unchanged). With several, a type
/// pinned via a provider-typed <c>UsingTransform</c> or <c>FromProvider(...)</c> on any of its mappings
/// must be modeled by that provider; otherwise the providers' models are probed via
/// <see cref="IWallabyModelProvider.Handles"/> and exactly one claimant must exist — none or several
/// fails fast with the fixes spelled out. A type mapped to several sinks resolves once: all its mappings
/// share a table, so they share a provider (conflicting pins are rejected at build time).
/// </summary>
internal static class ProviderAffinityResolver
{
    public static IReadOnlyDictionary<Type, string> Resolve(
        IEnumerable<MappingRegistration> mappings,
        IReadOnlyList<(string Name, IWallabyModelProvider Provider)> providers)
    {
        var affinities = new Dictionary<Type, string>();
        foreach (var group in mappings.GroupBy(m => m.EntityClrType))
        {
            affinities[group.Key] = providers.Count == 1
                ? providers[0].Name
                : ResolveOne(group.Key, group.Select(m => m.ProviderName).FirstOrDefault(n => n is not null), providers);
        }
        return affinities;
    }

    private static string ResolveOne(
        Type type, string? pinnedName, IReadOnlyList<(string Name, IWallabyModelProvider Provider)> providers)
    {
        if (pinnedName is not null)
        {
            // The builder validated the name is registered; here the pinned provider must actually model the type.
            var (name, provider) = providers.First(p => p.Name == pinnedName);
            if (!provider.Handles(type))
            {
                throw new WallabyConfigurationException(
                    $"Map<{type.Name}>() is pinned to provider '{name}', but that provider does not model " +
                    $"'{type.FullName}'.");
            }
            return name;
        }

        var candidates = providers.Where(p => p.Provider.Handles(type)).Select(p => p.Name).ToList();
        return candidates.Count switch
        {
            1 => candidates[0],
            0 => throw new WallabyConfigurationException(
                $"No registered storage provider models '{type.FullName}'. " +
                $"Registered providers: {Describe(providers.Select(p => p.Name))}."),
            _ => throw new WallabyConfigurationException(
                $"Multiple storage providers model '{type.FullName}': {Describe(candidates)}. " +
                $"Disambiguate with that provider's UsingTransform overload or Map<{type.Name}>().FromProvider(\"<name>\")."),
        };
    }

    private static string Describe(IEnumerable<string> names) => string.Join(", ", names.Select(n => $"'{n}'"));
}
