using Wallaby.DependencyInjection;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.SelfConfig;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Internal;

/// <summary>One registered provider, resolved: its capture plan and the enrichment sessions its mappings lease.</summary>
internal sealed class ResolvedProvider
{
    public required WallabyProviderRegistration Registration { get; init; }
    public required IWallabyModelProvider ModelProvider { get; init; }
    public required CapturePlan Plan { get; init; }

    /// <summary>The provider's enrichment sessions (the scoped override when set, else the default).</summary>
    public required IEnrichmentSessionProvider Sessions { get; init; }

    public string Name => Registration.Name;
}

/// <summary>
/// The registered providers resolved once at startup: per-provider capture plans plus the merged
/// <see cref="CapturePlan"/> the pipeline runs against. Merging at the plan level keeps ONE replication
/// slot/publication/checkpoint (global commit ordering, one ack LSN); the only per-provider concerns are
/// materializer dispatch (<see cref="CompositeRowMaterializer"/>) and enrichment sessions per mapping.
/// </summary>
internal sealed class ResolvedProviderSet
{
    private ResolvedProviderSet(
        IReadOnlyList<ResolvedProvider> providers,
        CapturePlan mergedPlan,
        IReadOnlyDictionary<Type, ResolvedProvider> providerByMappedType)
    {
        Providers = providers;
        MergedPlan = mergedPlan;
        ProviderByMappedType = providerByMappedType;
    }

    public IReadOnlyList<ResolvedProvider> Providers { get; }

    /// <summary>The merged model + dispatching materializer (the sole provider's plan when only one is registered).</summary>
    public CapturePlan MergedPlan { get; }

    /// <summary>Each mapping's resolved provider (by the mapping's entity CLR type).</summary>
    public IReadOnlyDictionary<Type, ResolvedProvider> ProviderByMappedType { get; }

    /// <summary>The model providers with their names, for external-slot <c>ForEntity&lt;T&gt;()</c> resolution.</summary>
    public IReadOnlyList<(string Name, IWallabyModelProvider Provider)> ModelProviders
        => [.. Providers.Select(p => (p.Name, p.ModelProvider))];

    public static ResolvedProviderSet Build(WallabyConfiguration config, IServiceProvider services)
    {
        if (config.Providers.Count == 0)
        {
            throw new WallabyConfigurationException(
                "Capturing requires a storage provider. Register one with " +
                "UseEntityFrameworkCore<TContext>() (from Wallaby.Providers.EntityFrameworkCore).");
        }

        var modelProviders = config.Providers
            .Select(registration => (registration.Name, Provider: registration.ModelProvider(services)))
            .ToList();
        var affinities = ProviderAffinityResolver.Resolve(config.AllMappings, modelProviders);

        var providers = new List<ResolvedProvider>(config.Providers.Count);
        foreach (var (registration, (_, modelProvider)) in config.Providers.Zip(modelProviders))
        {
            var plan = modelProvider.BuildCapturePlan(config.ToCaptureSpec(registration.Name, affinities));
            var sessions = (registration.ScopedEnrichmentSessions ?? registration.EnrichmentSessions)(services);
            providers.Add(new ResolvedProvider
            {
                Registration = registration,
                ModelProvider = modelProvider,
                Plan = plan,
                Sessions = sessions,
            });
        }

        var providerByMappedType = new Dictionary<Type, ResolvedProvider>();
        foreach (var (type, name) in affinities)
        {
            providerByMappedType[type] = providers.First(p => p.Name == name);
        }

        return new ResolvedProviderSet(providers, Merge(providers), providerByMappedType);
    }

    /// <summary>
    /// Merge the per-provider plans into one model, failing with named-provider errors before
    /// <see cref="WallabyModel"/>'s dictionaries would throw a bare <see cref="ArgumentException"/>.
    /// </summary>
    private static CapturePlan Merge(IReadOnlyList<ResolvedProvider> providers)
    {
        if (providers.Count == 1)
        {
            return providers[0].Plan;
        }

        var tables = new List<CapturedTable>();
        var bindings = new List<DependentBinding>();
        var tableOwners = new Dictionary<(string Schema, string Table), string>();
        var typeOwners = new Dictionary<Type, string>();
        foreach (var provider in providers)
        {
            foreach (var table in provider.Plan.Model.Tables)
            {
                if (!tableOwners.TryAdd((table.Schema, table.TableName), provider.Name))
                {
                    throw new WallabyConfigurationException(
                        $"Table '{table.QualifiedName}' is captured by both provider " +
                        $"'{tableOwners[(table.Schema, table.TableName)]}' and provider '{provider.Name}'. " +
                        "A table can only be captured by one provider.");
                }
                // Within one provider a CLR type may back several tables (primary + dependent); only a
                // cross-provider duplicate is a conflict.
                if (typeOwners.TryGetValue(table.EntityClrType, out var typeOwner) && typeOwner != provider.Name)
                {
                    throw new WallabyConfigurationException(
                        $"Entity type '{table.EntityClrType.FullName}' is captured by both provider " +
                        $"'{typeOwner}' and provider '{provider.Name}'. " +
                        "Pin its mapping with FromProvider(...) or narrow the providers' models.");
                }
                typeOwners[table.EntityClrType] = provider.Name;
                tables.Add(table);
            }
            bindings.AddRange(provider.Plan.Model.DependentBindings);
        }

        return new CapturePlan
        {
            Model = new WallabyModel(
                tables, bindings, [.. providers.SelectMany(p => p.Plan.Model.Warnings)]),
            Materializer = new CompositeRowMaterializer(providers.Select(p => p.Plan)),
        };
    }
}
