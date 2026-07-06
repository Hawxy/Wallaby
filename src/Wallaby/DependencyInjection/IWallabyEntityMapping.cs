namespace Wallaby.DependencyInjection;

/// <summary>
/// Non-generic plumbing for <see cref="SinkMappingBuilder.Apply(IWallabyEntityMapping)"/>; implement
/// <see cref="IWallabyEntityMapping{TEntity}"/> instead.
/// </summary>
public interface IWallabyEntityMapping
{
    /// <summary>Register this mapping's entity on the sink.</summary>
    void Apply(SinkMappingBuilder sink);
}

/// <summary>
/// One entity's sink mapping as a class instead of inline in the <c>AddWallaby</c> callback, so it can
/// live next to the transform it wires up:
/// <code>
/// public sealed class ProductSearchMapping : IWallabyEntityMapping&lt;Product&gt;
/// {
///     public void Configure(EntityMapBuilder&lt;Product&gt; map) => map
///         .ToDestination("products")
///         .UsingTransform&lt;Product, ProductSearchTransform&gt;();
/// }
/// </code>
/// Applied per sink via <see cref="SinkMappingBuilder.Apply{TMapping}"/>.
/// </summary>
public interface IWallabyEntityMapping<TEntity> : IWallabyEntityMapping where TEntity : class
{
    /// <summary>Configure the entity's mapping: destination, transform, backfill version, scoping, ...</summary>
    void Configure(EntityMapBuilder<TEntity> map);

    void IWallabyEntityMapping.Apply(SinkMappingBuilder sink) => Configure(sink.Map<TEntity>());
}
