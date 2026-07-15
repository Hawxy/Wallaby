using Wallaby.DependencyInjection;
using Wallaby.Providers;

namespace Wallaby.Tests.Unit;

/// <summary>
/// <c>ToCaptureSpec</c> aggregation of per-mapping column selections: every declared selection reaches
/// the spec, and one selection-less mapping keeps its entity at consume-all across all sinks.
/// </summary>
public class ColumnSelectionAggregationTests
{
    private sealed class Doc
    {
        public int Id { get; set; }
        public string? Sku { get; set; }
    }

    private static readonly Dictionary<Type, string> Affinity = new() { [typeof(Doc)] = "A" };

    private static WallabyConfiguration Config(params ColumnSelection?[] selectionPerMapping)
    {
        var config = new WallabyConfiguration();
        var sinkNumber = 0;
        foreach (var selection in selectionPerMapping)
        {
            var sink = new SinkRegistration { Name = $"sink{sinkNumber++}", Factory = _ => throw new NotSupportedException() };
            var mapping = new MappingRegistration { EntityClrType = typeof(Doc), ColumnSelection = selection };
            sink.Mappings.Add(mapping);
            config.Sinks.Add(sink);
        }
        return config;
    }

    [Test]
    public void Selections_from_all_mappings_reach_the_spec()
    {
        var config = Config(
            new ColumnSelection(ColumnSelectionMode.Include, [nameof(Doc.Sku)]),
            new ColumnSelection(ColumnSelectionMode.Exclude, [nameof(Doc.Sku)]));

        var spec = config.ToCaptureSpec("A", Affinity);

        spec.DeclaredColumnSelections[typeof(Doc)].Count.ShouldBe(2);
    }

    [Test]
    public void A_selection_less_mapping_keeps_the_entity_at_consume_all()
    {
        var config = Config(
            new ColumnSelection(ColumnSelectionMode.Include, [nameof(Doc.Sku)]),
            null);

        var spec = config.ToCaptureSpec("A", Affinity);

        spec.DeclaredColumnSelections.ShouldBeEmpty();
    }
}
