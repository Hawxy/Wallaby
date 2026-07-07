using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.Tests.Unit;

/// <summary>
/// Chain mechanics of the sink-scoped builder: sink registration returns a scope for attaching entity
/// mappings, and both continuation paths hand back the parent <see cref="WallabyBuilder"/>.
/// </summary>
public class WallabySinkBuilderTests
{
    private sealed class Doc { public int Id { get; set; } }

    private static (WallabyBuilder Builder, WallabySinkBuilder Sink) BuilderWithSink()
    {
        var builder = new WallabyBuilder();
        var sink = builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success));
        return (builder, sink);
    }

    [Test]
    public void WithMappings_returns_the_parent_builder()
    {
        var (builder, sink) = BuilderWithSink();

        sink.WithMappings(_ => { }).ShouldBeSameAs(builder);
    }

    [Test]
    public void The_Wallaby_property_continues_the_chain_without_mappings()
    {
        var (builder, sink) = BuilderWithSink();

        sink.Wallaby.ShouldBeSameAs(builder);
    }

    [Test]
    public void Mapping_the_same_entity_twice_in_one_sink_fails_fast()
    {
        var (_, sink) = BuilderWithSink();

        // Also across separate WithMappings calls — they extend the same sink.
        sink.WithMappings(s => s.Map<Doc>());
        var ex = Should.Throw<WallabyConfigurationException>(() => sink.WithMappings(s => s.Map<Doc>()));
        ex.Message.ShouldContain("'sink'");
        ex.Message.ShouldContain("at most once per sink");
    }

    private sealed class DocMapping : IWallabyEntityMapping<Doc>
    {
        public static EntityMapBuilder<Doc>? LastMap;
        public void Configure(EntityMapBuilder<Doc> map) => LastMap = map;
    }

    [Test]
    public void Apply_maps_the_entity_and_runs_the_configuration()
    {
        var (_, sink) = BuilderWithSink();
        DocMapping.LastMap = null;

        sink.WithMappings(s => s.Apply<DocMapping>());

        DocMapping.LastMap.ShouldNotBeNull();
        // The entity is registered on the sink: mapping it again collides.
        Should.Throw<WallabyConfigurationException>(() => sink.WithMappings(s => s.Map<Doc>()));
    }

    private sealed class OtherDoc { public int Id { get; set; } }

    private sealed class DelegatingMapping(Action<EntityMapBuilder<OtherDoc>> configure) : IWallabyEntityMapping<OtherDoc>
    {
        public void Configure(EntityMapBuilder<OtherDoc> map) => configure(map);
    }

    [Test]
    public void Apply_chains_and_accepts_configured_instances()
    {
        var (_, sink) = BuilderWithSink();
        var applied = false;

        sink.WithMappings(s => s
            .Apply<ChainedDocMapping>()
            .Apply(new DelegatingMapping(_ => applied = true)));

        applied.ShouldBeTrue();
        ChainedDocMapping.Applied.ShouldBeTrue();
    }

    private sealed class ChainedDocMapping : IWallabyEntityMapping<Doc>
    {
        public static bool Applied;
        public void Configure(EntityMapBuilder<Doc> map) => Applied = true;
    }

    [Test]
    public void Apply_rejects_a_null_mapping()
    {
        var (_, sink) = BuilderWithSink();

        sink.WithMappings(s => Should.Throw<ArgumentNullException>(() => s.Apply(null!)));
    }
}
