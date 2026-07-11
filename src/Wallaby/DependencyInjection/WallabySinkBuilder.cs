namespace Wallaby.DependencyInjection;

/// <summary>
/// Scope over one just-registered sink: attach its entity mappings via <see cref="WithMappings"/>, or
/// continue the Wallaby chain via <see cref="Wallaby"/> for a sink with no mappings.
/// </summary>
public sealed class WallabySinkBuilder
{
    private readonly WallabyBuilder _parent;
    private readonly SinkRegistration _sink;

    internal WallabySinkBuilder(WallabyBuilder parent, SinkRegistration sink)
    {
        _parent = parent;
        _sink = sink;
    }

    /// <summary>The parent builder, for continuing the chain without attaching mappings.</summary>
    public WallabyBuilder Wallaby => _parent;

    internal SinkRegistration Registration => _sink;

    /// <summary>
    /// Declare the entity mappings this sink receives: each <c>Map&lt;T&gt;()</c> inside the callback routes
    /// that entity's documents here. Repeated calls are additive. Returns the parent builder so the
    /// Wallaby chain continues.
    /// </summary>
    public WallabyBuilder WithMappings(Action<SinkMappingBuilder> map)
    {
        ArgumentNullException.ThrowIfNull(map);
        map(new SinkMappingBuilder(_sink));
        return _parent;
    }
}

/// <summary>Declares the entity mappings of one sink (the argument of <see cref="WallabySinkBuilder.WithMappings"/>).</summary>
public sealed class SinkMappingBuilder
{
    private readonly SinkRegistration _sink;

    internal SinkMappingBuilder(SinkRegistration sink) => _sink = sink;

    /// <summary>
    /// Map an entity to this sink: declares the entity's table for capture and configures how its documents
    /// are shaped and addressed (<c>UsingTransform</c>, <c>ToDestination</c>, ...). An entity maps at most
    /// once per sink; the same entity may be mapped under several sinks, each mapping running its own
    /// transform.
    /// </summary>
    public EntityMapBuilder<TEntity> Map<TEntity>() where TEntity : class
    {
        if (_sink.Mappings.Any(m => m.EntityClrType == typeof(TEntity)))
        {
            throw new WallabyConfigurationException(
                $"Sink '{_sink.Name}' already maps {typeof(TEntity).Name}. An entity maps at most once per " +
                "sink; route it to another sink or fold the difference into the mapping's transform.");
        }

        var registration = new MappingRegistration { EntityClrType = typeof(TEntity) };
        _sink.Mappings.Add(registration);
        return new EntityMapBuilder<TEntity>(registration);
    }

    /// <summary>
    /// Apply a mapping configuration class — equivalent to <see cref="Map{TEntity}"/> followed by the
    /// class's <c>Configure(...)</c> calls. Returns this builder so applications chain.
    /// </summary>
    public SinkMappingBuilder Apply<TMapping>() where TMapping : IWallabyEntityMapping, new()
        => Apply(new TMapping());

    /// <summary>Apply a mapping configuration instance (for mappings with constructor arguments).</summary>
    public SinkMappingBuilder Apply(IWallabyEntityMapping mapping)
    {
        ArgumentNullException.ThrowIfNull(mapping);
        mapping.Apply(this);
        return this;
    }
}
