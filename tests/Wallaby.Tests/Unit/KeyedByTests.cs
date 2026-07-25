using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.Tests.Unit;

/// <summary>
/// <c>KeyedBy(...)</c>/<c>ScopedBy(...)</c> delete-time identity semantics: the mapping requires FULL
/// replica identity (the custom id must be computable on deletes), a selector that comes up empty fails
/// loudly with the DDL to fix it, and an entity-less row fails loudly instead of falling back to the
/// primary key (a PK-named document was never written, so that delete would remove nothing).
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
    public void Keyed_by_without_entity_fails_loudly()
    {
        var selector = Selector(m => m.KeyedBy(d => d.Sku!));

        // A PK fallback would target a document that was never written; the custom-keyed orphan lingers.
        var ex = Should.Throw<InvalidOperationException>(() => selector(Change(entity: null)));

        ex.Message.ShouldContain("public.docs");
        ex.Message.ShouldContain("REPLICA IDENTITY FULL");
    }

    [Test]
    public void Entity_scoped_key_without_entity_is_null_for_enrichment_only_scoping()
    {
        var mapping = new MappingRegistration { EntityClrType = typeof(Doc) };
        new EntityMapBuilder<Doc>(mapping).ScopedBy(d => d.Sku);

        mapping.ScopeKeySelector.ShouldNotBeNull()(Change(entity: null)).ShouldBeNull();
    }

    [Test]
    public void Entity_scoped_key_without_entity_fails_when_a_scoped_destination_depends_on_it()
    {
        var mapping = new MappingRegistration { EntityClrType = typeof(Doc) };
        new EntityMapBuilder<Doc>(mapping)
            .ScopedBy(d => d.Sku)
            .ScopedDestination(key => $"idx_{key}");

        var ex = Should.Throw<InvalidOperationException>(
            () => mapping.ScopeKeySelector.ShouldNotBeNull()(Change(entity: null)));

        ex.Message.ShouldContain("ScopedDestination");
        ex.Message.ShouldContain("REPLICA IDENTITY FULL");
    }

    [Test]
    public void Requires_materialized_entity_covers_keyed_by_and_entity_scoped_destinations_only()
    {
        Spec(m => m.KeyedBy(d => d.Sku!)).RequiresMaterializedEntity.ShouldBe([typeof(Doc)]);
        Spec(m => m.ScopedBy(d => d.Sku).ScopedDestination(k => $"idx_{k}"))
            .RequiresMaterializedEntity.ShouldBe([typeof(Doc)]);

        // Enrichment-only entity scoping and record-based scoping never need the entity on delete.
        Spec(m => m.ScopedBy(d => d.Sku)).RequiresMaterializedEntity.ShouldBeEmpty();
        Spec(m => m.ScopedBy(c => c.Record.GetValueOrDefault("TenantId")).ScopedDestination(k => $"idx_{k}"))
            .RequiresMaterializedEntity.ShouldBeEmpty();
    }

    private static Wallaby.Providers.CaptureSpec Spec(Action<EntityMapBuilder<Doc>> configure)
    {
        var config = new WallabyConfiguration();
        var sink = new SinkRegistration { Name = "sink", Factory = _ => throw new NotSupportedException() };
        var mapping = new MappingRegistration { EntityClrType = typeof(Doc) };
        configure(new EntityMapBuilder<Doc>(mapping));
        sink.Mappings.Add(mapping);
        config.Sinks.Add(sink);
        return config.ToCaptureSpec("A", new Dictionary<Type, string> { [typeof(Doc)] = "A" });
    }
}
