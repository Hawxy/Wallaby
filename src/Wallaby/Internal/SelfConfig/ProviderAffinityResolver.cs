using Wallaby.DependencyInjection;
using Wallaby.Providers;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Assigns each mapping to a storage provider at startup. With one registered provider everything resolves
/// to it directly (its own model errors surface unchanged). With several, a mapping pinned via a
/// provider-typed <c>UsingTransform</c> or <c>FromProvider(...)</c> must be modeled by that provider;
/// otherwise the providers' models are probed via <see cref="IWallabyModelProvider.Handles"/> and exactly
/// one claimant must exist — none or several fails fast with the fixes spelled out.
/// </summary>
internal static class ProviderAffinityResolver
{
    public static IReadOnlyDictionary<Type, string> Resolve(
        IReadOnlyCollection<MappingRegistration> mappings,
        IReadOnlyList<(string Name, IWallabyModelProvider Provider)> providers)
    {
        var affinities = new Dictionary<Type, string>();
        foreach (var mapping in mappings)
        {
            affinities[mapping.EntityClrType] = providers.Count == 1
                ? providers[0].Name
                : ResolveOne(mapping, providers);
        }
        return affinities;
    }

    private static string ResolveOne(
        MappingRegistration mapping, IReadOnlyList<(string Name, IWallabyModelProvider Provider)> providers)
    {
        var type = mapping.EntityClrType;
        if (mapping.ProviderName is not null)
        {
            // The builder validated the name is registered; here the pinned provider must actually model the type.
            var (name, provider) = providers.First(p => p.Name == mapping.ProviderName);
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
