using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.UnitTests;

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
}
