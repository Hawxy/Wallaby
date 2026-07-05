using Wallaby.DependencyInjection;
using Wallaby.Marten.Internal;

namespace Wallaby.Marten;

/// <summary>Marten transform registration for entity mappings.</summary>
public static class MartenEntityMapBuilderExtensions
{
    /// <summary>Use a transform instance.</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity>(
        this EntityMapBuilder<TEntity> map, IWallabyMartenTransform<TEntity> transform)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(transform);
        return map.UsingTransformInvoker(_ => new MartenTransformInvoker<TEntity>(transform));
    }
}
