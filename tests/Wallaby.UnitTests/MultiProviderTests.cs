using System.Diagnostics.CodeAnalysis;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Internal;
using Wallaby.Internal.Pipeline;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.UnitTests;

/// <summary>
/// Multi-provider core semantics over two in-test fake providers: mapping→provider affinity resolution,
/// per-provider capture specs, plan merging (with friendly conflict errors), composite materializer
/// dispatch, and per-mapping enrichment-session leasing in the router.
/// </summary>
public class MultiProviderTests
{
    private sealed class Alpha { public int Id { get; set; } }
    private sealed class Beta { public int Id { get; set; } }
    private sealed class Shared { public int Id { get; set; } }

    // ---- fakes ----

    /// <summary>Materializer that stamps rows with its provider's name so dispatch is observable.</summary>
    private sealed class FakeMaterializer(string providerName, IReadOnlyList<CapturedTable> tables) : IRowMaterializer
    {
        public bool TryMaterialize(RawChange change, [NotNullWhen(true)] out MaterializedRow? row)
        {
            var table = tables.FirstOrDefault(t => t.Schema == change.Schema && t.TableName == change.TableName);
            if (table is null)
            {
                row = null;
                return false;
            }
            row = new MaterializedRow(
                change.Action, providerName, new Dictionary<string, object?>(), null, [1], table.EntityClrType);
            return true;
        }
    }

    /// <summary>A hand-built provider over (type, table) pairs; records the spec it was asked to plan.</summary>
    private sealed class FakeModelProvider(string name, params (Type Type, string Table)[] entities) : IWallabyModelProvider
    {
        public string Name => name;

        public CaptureSpec? LastSpec { get; private set; }

        /// <summary>Plan every modeled table regardless of the spec (simulates a misbehaving provider).</summary>
        public bool IgnoresDeclaredEntities { get; init; }

        public CapturePlan BuildCapturePlan(CaptureSpec spec)
        {
            LastSpec = spec;
            var declared = IgnoresDeclaredEntities
                ? entities
                : entities.Where(e => spec.DeclaredEntities.Contains(e.Type)).ToArray();
            var tables = declared.Select(e => Table(e.Type, e.Table)).ToList();
            return new CapturePlan { Model = new WallabyModel(tables), Materializer = new FakeMaterializer(name, tables) };
        }

        public QualifiedTable ResolveTable(Type entityClrType)
        {
            var entity = entities.FirstOrDefault(e => e.Type == entityClrType);
            return entity.Type is null
                ? throw new WallabyConfigurationException($"'{entityClrType.FullName}' is not modeled by '{name}'.")
                : new QualifiedTable("public", entity.Table);
        }

        public bool Handles(Type entityClrType) => entities.Any(e => e.Type == entityClrType);

        private static CapturedTable Table(Type type, string table)
        {
            var id = new CapturedColumn { PropertyName = "Id", ColumnName = "id", ClrType = typeof(int), IsPrimaryKey = true };
            return new CapturedTable
            {
                EntityClrType = type, Schema = "public", TableName = table, Columns = [id], PrimaryKey = [id],
            };
        }
    }

    private sealed class FakeSession(FakeSessionProvider owner) : IEnrichmentSession
    {
        public object Session => owner;
        public ValueTask DisposeAsync()
        {
            owner.Disposals++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FakeSessionProvider : IEnrichmentSessionProvider
    {
        public int Leases { get; private set; }
        public int Disposals { get; set; }
        public bool IsScoped => false;

        public IEnrichmentSession Lease(object? scopeKey)
        {
            Leases++;
            return new FakeSession(this);
        }
    }

    /// <summary>Transform that records the session each invocation received and upserts every change.</summary>
    private sealed class RecordingTransform : IWallabyTransformInvoker
    {
        public List<object> Sessions { get; } = [];

        public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
            object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
        {
            Sessions.Add(session);
            var documents = new Dictionary<DocumentKey, WallabyDocument?>();
            foreach (var change in changes)
            {
                documents[change.Key] = new WallabyDocument { ["id"] = change.Key.ToString() };
            }
            return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
        }
    }

    private sealed class NullServiceProvider : IServiceProvider
    {
        public static readonly NullServiceProvider Instance = new();
        public object? GetService(Type serviceType) => null;
    }

    private static WallabyProviderRegistration Registration(FakeModelProvider provider, FakeSessionProvider? sessions = null)
        => new()
        {
            Name = provider.Name,
            ModelProvider = _ => provider,
            EnrichmentSessions = _ => sessions ?? new FakeSessionProvider(),
        };

    private static WallabyBuilder CapturingBuilder(params WallabyProviderRegistration[] providers)
    {
        var builder = new WallabyBuilder();
        builder.UseConnectionString("Host=localhost;Database=db;Username=u;Password=p");
        builder.AddDelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success));
        foreach (var provider in providers)
        {
            builder.UseProvider(provider);
        }
        return builder;
    }

    private static EntityMapBuilder<TEntity> Map<TEntity>(WallabyBuilder builder) where TEntity : class
        => builder.Map<TEntity>().ToSink("sink").UsingTransformInvoker(_ => new RecordingTransform());

    private static ResolvedProviderSet Resolve(WallabyBuilder builder)
        => ResolvedProviderSet.Build(builder.Build(), NullServiceProvider.Instance);

    // ---- registration ----

    [Test]
    public void Two_providers_with_the_same_name_are_rejected()
    {
        var builder = CapturingBuilder(Registration(new FakeModelProvider("A", (typeof(Alpha), "alpha"))));

        Should.Throw<WallabyConfigurationException>(
                () => builder.UseProvider(Registration(new FakeModelProvider("A", (typeof(Beta), "beta")))))
            .Message.ShouldContain("'A'");
    }

    [Test]
    public void Scoped_sessions_require_the_target_provider_to_be_registered_first()
    {
        var builder = new WallabyBuilder();

        Should.Throw<WallabyConfigurationException>(
            () => builder.UseScopedEnrichmentSessions("A", _ => new FakeSessionProvider()));
    }

    // ---- affinity resolution ----

    [Test]
    public void Mappings_auto_resolve_to_the_sole_provider_that_models_their_type()
    {
        var a = new FakeModelProvider("A", (typeof(Alpha), "alpha"));
        var b = new FakeModelProvider("B", (typeof(Beta), "beta"));
        var builder = CapturingBuilder(Registration(a), Registration(b));
        Map<Alpha>(builder);
        Map<Beta>(builder);

        var resolved = Resolve(builder);

        resolved.ProviderByMappedType[typeof(Alpha)].Name.ShouldBe("A");
        resolved.ProviderByMappedType[typeof(Beta)].Name.ShouldBe("B");
        a.LastSpec!.DeclaredEntities.ShouldBe(new[] { typeof(Alpha) });
        b.LastSpec!.DeclaredEntities.ShouldBe(new[] { typeof(Beta) });
    }

    [Test]
    public void A_type_no_provider_models_fails_fast()
    {
        var builder = CapturingBuilder(
            Registration(new FakeModelProvider("A", (typeof(Alpha), "alpha"))),
            Registration(new FakeModelProvider("B", (typeof(Beta), "beta"))));
        Map<Shared>(builder);

        Should.Throw<WallabyConfigurationException>(() => Resolve(builder))
            .Message.ShouldContain("No registered storage provider models");
    }

    [Test]
    public void A_type_both_providers_model_fails_fast_and_names_the_fixes()
    {
        var builder = CapturingBuilder(
            Registration(new FakeModelProvider("A", (typeof(Shared), "shared_a"))),
            Registration(new FakeModelProvider("B", (typeof(Shared), "shared_b"))));
        Map<Shared>(builder);

        var ex = Should.Throw<WallabyConfigurationException>(() => Resolve(builder));
        ex.Message.ShouldContain("Multiple storage providers model");
        ex.Message.ShouldContain("FromProvider");
    }

    [Test]
    public void FromProvider_breaks_a_tie_explicitly()
    {
        var a = new FakeModelProvider("A", (typeof(Shared), "shared_a"));
        var b = new FakeModelProvider("B", (typeof(Shared), "shared_b"));
        var builder = CapturingBuilder(Registration(a), Registration(b));
        Map<Shared>(builder).FromProvider("B");

        var resolved = Resolve(builder);

        resolved.ProviderByMappedType[typeof(Shared)].Name.ShouldBe("B");
        b.LastSpec!.DeclaredEntities.ShouldBe(new[] { typeof(Shared) });
        a.LastSpec!.DeclaredEntities.ShouldBeEmpty();
    }

    [Test]
    public void A_provider_typed_transform_breaks_a_tie()
    {
        var a = new FakeModelProvider("A", (typeof(Shared), "shared_a"));
        var b = new FakeModelProvider("B", (typeof(Shared), "shared_b"));
        var builder = CapturingBuilder(Registration(a), Registration(b));
        builder.Map<Shared>().ToSink("sink")
            .UsingTransformInvoker(_ => new RecordingTransform(), providerName: "A");

        Resolve(builder).ProviderByMappedType[typeof(Shared)].Name.ShouldBe("A");
    }

    [Test]
    public void FromProvider_conflicting_with_the_transforms_provider_fails_at_build()
    {
        var builder = CapturingBuilder(
            Registration(new FakeModelProvider("A", (typeof(Shared), "shared_a"))),
            Registration(new FakeModelProvider("B", (typeof(Shared), "shared_b"))));
        builder.Map<Shared>().ToSink("sink")
            .UsingTransformInvoker(_ => new RecordingTransform(), providerName: "A")
            .FromProvider("B");

        Should.Throw<WallabyConfigurationException>(() => builder.Build())
            .Message.ShouldContain("pinned to provider 'B'");
    }

    [Test]
    public void FromProvider_naming_an_unregistered_provider_fails_at_build()
    {
        var builder = CapturingBuilder(Registration(new FakeModelProvider("A", (typeof(Alpha), "alpha"))));
        Map<Alpha>(builder).FromProvider("Nope");

        Should.Throw<WallabyConfigurationException>(() => builder.Build())
            .Message.ShouldContain("'Nope'");
    }

    [Test]
    public void FromProvider_naming_a_provider_that_does_not_model_the_type_fails()
    {
        var builder = CapturingBuilder(
            Registration(new FakeModelProvider("A", (typeof(Alpha), "alpha"))),
            Registration(new FakeModelProvider("B", (typeof(Beta), "beta"))));
        Map<Alpha>(builder).FromProvider("B");

        Should.Throw<WallabyConfigurationException>(() => Resolve(builder))
            .Message.ShouldContain("does not model");
    }

    [Test]
    public void A_single_provider_takes_every_mapping_without_probing()
    {
        // Handles(...) is false for Beta, but with one provider everything resolves to it directly, so
        // the provider's own model surfaces its (unchanged) error at plan time instead of the resolver.
        var a = new FakeModelProvider("A", (typeof(Alpha), "alpha"));
        var builder = CapturingBuilder(Registration(a));
        Map<Alpha>(builder);
        Map<Beta>(builder);

        var resolved = Resolve(builder);

        resolved.ProviderByMappedType[typeof(Beta)].Name.ShouldBe("A");
        a.LastSpec!.DeclaredEntities.ShouldBe(new[] { typeof(Alpha), typeof(Beta) }, ignoreOrder: true);
    }

    // ---- per-provider capture specs ----

    [Test]
    public void Replica_identity_flags_partition_by_provider()
    {
        var a = new FakeModelProvider("A", (typeof(Alpha), "alpha"));
        var b = new FakeModelProvider("B", (typeof(Beta), "beta"));
        var builder = CapturingBuilder(Registration(a), Registration(b));
        builder.UseScopedEnrichmentSessions("A", _ => new FakeSessionProvider());
        Map<Alpha>(builder).ScopedBy((ChangeEvent c) => "t").ScopedDestination(k => $"dest-{k}");
        Map<Beta>(builder);

        Resolve(builder);

        a.LastSpec!.RequiresFullReplicaIdentity.ShouldHaveSingleItem().ShouldBe(typeof(Alpha));
        b.LastSpec!.RequiresFullReplicaIdentity.ShouldBeEmpty();
    }

    // ---- plan merging ----

    [Test]
    public void The_merged_model_contains_both_providers_tables()
    {
        var builder = CapturingBuilder(
            Registration(new FakeModelProvider("A", (typeof(Alpha), "alpha"))),
            Registration(new FakeModelProvider("B", (typeof(Beta), "beta"))));
        Map<Alpha>(builder);
        Map<Beta>(builder);

        var merged = Resolve(builder).MergedPlan.Model;

        merged.FindByRelation("public", "alpha")!.EntityClrType.ShouldBe(typeof(Alpha));
        merged.FindByRelation("public", "beta")!.EntityClrType.ShouldBe(typeof(Beta));
    }

    [Test]
    public void A_table_captured_by_two_providers_fails_with_both_names()
    {
        var builder = CapturingBuilder(
            Registration(new FakeModelProvider("A", (typeof(Alpha), "same_table"))),
            Registration(new FakeModelProvider("B", (typeof(Beta), "same_table"))));
        Map<Alpha>(builder);
        Map<Beta>(builder);

        var ex = Should.Throw<WallabyConfigurationException>(() => Resolve(builder));
        ex.Message.ShouldContain("public.same_table");
        ex.Message.ShouldContain("'A'");
        ex.Message.ShouldContain("'B'");
    }

    [Test]
    public void A_clr_type_captured_by_two_providers_fails_with_both_names()
    {
        // Different tables, same CLR type: only reachable when a provider plans tables beyond its
        // declared entities, so simulate one that ignores the spec.
        var builder = CapturingBuilder(
            Registration(new FakeModelProvider("A", (typeof(Shared), "shared_a"))),
            Registration(new FakeModelProvider("B", (typeof(Shared), "shared_b")) { IgnoresDeclaredEntities = true }));
        Map<Shared>(builder).FromProvider("A");

        var ex = Should.Throw<WallabyConfigurationException>(() => Resolve(builder));
        ex.Message.ShouldContain(typeof(Shared).FullName!);
        ex.Message.ShouldContain("'A'");
        ex.Message.ShouldContain("'B'");
    }

    [Test]
    public void A_single_providers_plan_is_passed_through_unmerged()
    {
        var a = new FakeModelProvider("A", (typeof(Alpha), "alpha"));
        var builder = CapturingBuilder(Registration(a));
        Map<Alpha>(builder);

        var resolved = Resolve(builder);

        resolved.MergedPlan.ShouldBeSameAs(resolved.Providers[0].Plan);
        resolved.MergedPlan.Materializer.ShouldBeOfType<FakeMaterializer>();
    }

    // ---- composite materializer ----

    [Test]
    public void The_composite_materializer_dispatches_by_table_to_the_owning_provider()
    {
        var builder = CapturingBuilder(
            Registration(new FakeModelProvider("A", (typeof(Alpha), "alpha"))),
            Registration(new FakeModelProvider("B", (typeof(Beta), "beta"))));
        Map<Alpha>(builder);
        Map<Beta>(builder);

        var materializer = Resolve(builder).MergedPlan.Materializer;
        materializer.ShouldBeOfType<CompositeRowMaterializer>();

        materializer.TryMaterialize(Raw("alpha"), out var alphaRow).ShouldBeTrue();
        alphaRow!.Entity.ShouldBe("A"); // FakeMaterializer stamps its provider name
        materializer.TryMaterialize(Raw("beta"), out var betaRow).ShouldBeTrue();
        betaRow!.Entity.ShouldBe("B");
        materializer.TryMaterialize(Raw("unknown"), out _).ShouldBeFalse();

        static RawChange Raw(string table) => new()
        {
            RelationId = 1, Schema = "public", TableName = table, Action = ChangeAction.Insert,
        };
    }

    // ---- enrichment sessions ----

    [Test]
    public void The_scoped_override_wins_for_its_provider_only()
    {
        var scoped = new FakeSessionProvider();
        var aDefault = new FakeSessionProvider();
        var bDefault = new FakeSessionProvider();
        var builder = CapturingBuilder(
            Registration(new FakeModelProvider("A", (typeof(Alpha), "alpha")), aDefault),
            Registration(new FakeModelProvider("B", (typeof(Beta), "beta")), bDefault));
        builder.UseScopedEnrichmentSessions("A", _ => scoped);
        Map<Alpha>(builder);
        Map<Beta>(builder);

        var resolved = Resolve(builder);

        resolved.ProviderByMappedType[typeof(Alpha)].Sessions.ShouldBeSameAs(scoped);
        resolved.ProviderByMappedType[typeof(Beta)].Sessions.ShouldBeSameAs(bDefault);
    }

    [Test]
    public async Task The_router_leases_one_session_per_provider_per_batch()
    {
        var aSessions = new FakeSessionProvider();
        var bSessions = new FakeSessionProvider();
        var aTransform = new RecordingTransform();
        var bTransform = new RecordingTransform();
        var router = new MappingChangeRouter(new Dictionary<Type, EntityMapping>
        {
            [typeof(Alpha)] = Mapping(typeof(Alpha), aTransform, aSessions),
            [typeof(Beta)] = Mapping(typeof(Beta), bTransform, bSessions),
        });

        var routed = await router.RouteAsync(
            [Change(typeof(Alpha), 1), Change(typeof(Beta), 2), Change(typeof(Alpha), 3)], CancellationToken.None);

        routed.Count.ShouldBe(3);
        aSessions.Leases.ShouldBe(1);
        bSessions.Leases.ShouldBe(1);
        aTransform.Sessions.ShouldHaveSingleItem().ShouldBeSameAs(aSessions);
        bTransform.Sessions.ShouldHaveSingleItem().ShouldBeSameAs(bSessions);
        aSessions.Disposals.ShouldBe(1);
        bSessions.Disposals.ShouldBe(1);
    }

    [Test]
    public async Task Mappings_on_the_same_provider_share_one_session_per_batch()
    {
        var sessions = new FakeSessionProvider();
        var router = new MappingChangeRouter(new Dictionary<Type, EntityMapping>
        {
            [typeof(Alpha)] = Mapping(typeof(Alpha), new RecordingTransform(), sessions),
            [typeof(Beta)] = Mapping(typeof(Beta), new RecordingTransform(), sessions),
        });

        await router.RouteAsync([Change(typeof(Alpha), 1), Change(typeof(Beta), 2)], CancellationToken.None);

        sessions.Leases.ShouldBe(1);
        sessions.Disposals.ShouldBe(1);
    }

    private static EntityMapping Mapping(Type type, IWallabyTransformInvoker transform, FakeSessionProvider sessions)
        => new()
        {
            EntityClrType = type, SinkName = "sink", Destination = "dest", Transform = transform, Sessions = sessions,
        };

    private static ChangeEvent Change(Type type, int id)
    {
        var meta = new ChangeMetadata("public", "t", DateTimeOffset.UtcNow, 1, 0, IsBackfill: false);
        return new ChangeEvent(
            ChangeAction.Insert, meta, Entity: id, new Dictionary<string, object?>(), Changes: null, [id])
        {
            EntityClrType = type,
        };
    }
}
