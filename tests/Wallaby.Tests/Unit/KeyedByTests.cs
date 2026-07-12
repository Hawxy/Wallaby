using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.Tests.Unit;

/// <summary>
/// <c>KeyedBy(...)</c> semantics: the mapping requires FULL replica identity (the custom id must be
/// computable on deletes), a selector that comes up empty fails loudly with the DDL to fix it, and
/// entity-less key-only rows fall back to the primary key.
/// </summary>
public class KeyedByTests
{
    private sealed class Doc
    {
        public int Id { get; set; }
        public string? Sku { get; set; }
    }

    private static Func<ChangeEvent, string> Selector(Action<EntityMapBuilder<Doc>> configure)
    {
        var mapping = new MappingRegistration { EntityClrType = typeof(Doc) };
        configure(new EntityMapBuilder<Doc>(mapping));
        return mapping.DocumentIdSelector.ShouldNotBeNull();
    }

    private static ChangeEvent Change(object? entity, ChangeAction action = ChangeAction.Delete)
    {
        var meta = new ChangeMetadata("public", "docs", action, DateTimeOffset.UtcNow, 1, 0, IsBackfill: false);
        return new ChangeEvent(action, meta, entity, new Dictionary<string, object?>(), Changes: null, [7])
        {
            EntityClrType = typeof(Doc),
        };
    }

    [Test]
    public void Keyed_by_marks_requires_full_replica_identity()
    {
        var config = new WallabyConfiguration();
        var sink = new SinkRegistration { Name = "sink", Factory = _ => throw new NotSupportedException() };
        var mapping = new MappingRegistration { EntityClrType = typeof(Doc) };
        new EntityMapBuilder<Doc>(mapping).KeyedBy(d => d.Sku!);
        sink.Mappings.Add(mapping);
        config.Sinks.Add(sink);

        var spec = config.ToCaptureSpec("A", new Dictionary<Type, string> { [typeof(Doc)] = "A" });

        spec.RequiresFullReplicaIdentity.ShouldHaveSingleItem().ShouldBe(typeof(Doc));
    }

    [Test]
    public void Keyed_by_selector_returning_null_throws_with_replica_identity_guidance()
    {
        var selector = Selector(m => m.KeyedBy(d => d.Sku!));

        var ex = Should.Throw<InvalidOperationException>(
            () => selector(Change(new Doc { Id = 7, Sku = null })));

        ex.Message.ShouldContain("public.docs");
        ex.Message.ShouldContain("ALTER TABLE public.docs REPLICA IDENTITY FULL;");
    }

    [Test]
    public void Keyed_by_uses_the_selector_value()
    {
        var selector = Selector(m => m.KeyedBy(d => d.Sku!));

        selector(Change(new Doc { Id = 7, Sku = "sku-7" })).ShouldBe("sku-7");
    }

    [Test]
    public void Keyed_by_without_entity_falls_back_to_primary_key()
    {
        var selector = Selector(m => m.KeyedBy(d => d.Sku!));

        // Key-only rows (e.g. Marten deletes) carry no entity; the PK identifies the document.
        selector(Change(entity: null)).ShouldBe("7");
    }
}
