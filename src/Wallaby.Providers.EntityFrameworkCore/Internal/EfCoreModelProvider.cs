using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Wallaby.Providers;

namespace Wallaby.Providers.EntityFrameworkCore.Internal;

/// <summary>
/// The EF Core storage provider: derives the capture plan from the consumer's <see cref="IModel"/>
/// (via <see cref="EfCoreCaptureModelBuilder"/>) and materializes rows with <see cref="EntityMaterializer"/>.
/// The materializer plans every table in the model — not just captured ones — so publication-included
/// but unmapped tables still materialize and are skipped by the router.
/// </summary>
internal sealed class EfCoreModelProvider(IModel model) : IWallabyModelProvider
{
    public CapturePlan BuildCapturePlan(CaptureSpec spec)
    {
        var consumedProperties = ColumnConsumptionResolver.Resolve(model, spec);
        var captureModel = EfCoreCaptureModelBuilder.Build(model, spec, consumedProperties);
        return new()
        {
            Model = captureModel,
            // Captured tables materialize strictly: an owned member that cannot be constructed fails
            // startup instead of the first row.
            Materializer = new EntityMaterializer(
                model, consumedProperties,
                capturedTypes: captureModel.Tables.Select(t => t.EntityClrType).ToHashSet()),
        };
    }

    public QualifiedTable ResolveTable(Type entityClrType)
    {
        var entityType = model.FindEntityType(entityClrType)
            ?? throw new WallabyConfigurationException(
                $"'{entityClrType.FullName}' is not part of the EF Core model.");
        var tableName = entityType.GetTableName()
            ?? throw new WallabyConfigurationException(
                $"'{entityClrType.FullName}' is not mapped to a table.");
        var table = new QualifiedTable(entityType.GetSchema() ?? "public", tableName);

        // An owned type stored in its owner's table (same-table or JSON) has no table of its own; naming it
        // would silently target the owner's table.
        if (entityType.FindOwnership()?.PrincipalEntityType is { } owner
            && owner.GetTableName() == tableName && (owner.GetSchema() ?? "public") == table.Schema)
        {
            throw new WallabyConfigurationException(
                $"'{entityClrType.FullName}' is stored in its owner's table '{table.Schema}.{table.Table}'; name the owner instead.");
        }

        return table;
    }

    // The relational model is EF's own physical-table view: TPH, same-table owned and JSON-mapped types
    // collapse onto one table, while TPT/TPC, split and separate-table owned types and many-to-many join
    // tables each appear. Views live separately, and keyless tables carry no primary key.
    public IReadOnlyList<QualifiedTable> ResolveAllTables()
        => model.GetRelationalModel().Tables
            .Where(t => t.PrimaryKey is not null)
            .Select(t => new QualifiedTable(t.Schema ?? "public", t.Name))
            .Distinct()
            .ToList();

    public bool Handles(Type entityClrType)
        => model.FindEntityType(entityClrType)?.GetTableName() is not null;
}
