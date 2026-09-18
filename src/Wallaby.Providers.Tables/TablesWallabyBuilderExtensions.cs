using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.DependencyInjection;
using Wallaby.Internal;
using Wallaby.Providers.Tables.Internal;

namespace Wallaby.Providers.Tables;

/// <summary>Plain-table provider registration for the Wallaby builder.</summary>
public static class TablesWallabyBuilderExtensions
{
    /// <summary>The provider name this package registers under, for <c>Map&lt;T&gt;().FromProvider(...)</c>.</summary>
    public const string ProviderName = "Tables";

    /// <summary>
    /// Drive capture from POCOs registered in <paramref name="configure"/>, with no ORM in between.
    /// Transforms receive the <see cref="NpgsqlDataSource"/> registered in the container
    /// (<c>AddNpgsqlDataSource</c>) when there is one, else the data source Wallaby builds from its own
    /// connection string.
    /// </summary>
    public static WallabyBuilder UseTables(this WallabyBuilder builder, Action<TablesModelBuilder> configure)
        => builder.UseTables(
            sp => sp.GetService<NpgsqlDataSource>() ?? sp.GetRequiredService<WallabyDataSource>().Source,
            configure);

    /// <summary>
    /// <see cref="UseTables(WallabyBuilder, Action{TablesModelBuilder})"/> with an explicit data source for
    /// transforms, e.g. a keyed registration or one pointed at a read replica.
    /// </summary>
    public static WallabyBuilder UseTables(
        this WallabyBuilder builder, Func<IServiceProvider, NpgsqlDataSource> dataSource, Action<TablesModelBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(configure);

        var model = new TablesModelBuilder();
        configure(model);
        var registrations = model.Build();
        return builder.UseProvider(new WallabyProviderRegistration
        {
            Name = ProviderName,
            ModelProvider = _ => new TablesModelProvider(registrations),
            EnrichmentSessions = sp => new TablesEnrichmentSessionProvider(dataSource(sp)),
        });
    }
}
