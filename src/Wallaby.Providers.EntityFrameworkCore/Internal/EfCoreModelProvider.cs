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
        return new QualifiedTable(entityType.GetSchema() ?? "public", tableName);
    }

    public bool Handles(Type entityClrType)
        => model.FindEntityType(entityClrType)?.GetTableName() is not null;
}
