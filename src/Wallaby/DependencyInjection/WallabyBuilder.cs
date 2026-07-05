using System.ComponentModel;
using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Providers;
using Wallaby.Sinks;

namespace Wallaby.DependencyInjection;

/// <summary>Fluent configuration for a Wallaby instance.</summary>
public sealed class WallabyBuilder
{
    private readonly WallabyConfiguration _configuration = new();

    /// <summary>
    /// Configure options (slot/publication names, chunk size, auto-backfill, etc.). The action joins the
    /// standard options pipeline at the <c>AddWallaby</c> registration position, so it composes with
    /// <c>services.Configure&lt;WallabyOptions&gt;()</c>/<c>PostConfigure</c> calls: earlier <c>Configure</c>
    /// registrations run before it, later ones and <c>PostConfigure</c> override it.
    /// </summary>
    public WallabyBuilder ConfigureOptions(Action<WallabyOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _configuration.OptionsActions.Add(configure);
        return this;
    }

    /// <summary>
    /// Postgres connection string used for replication, checkpoint storage, advisory locks, and backfill reads.
    /// Shorthand for <c>ConfigureOptions(o =&gt; o.ConnectionString = ...)</c> — like any option value it can
    /// also be supplied (or overridden) through <c>Configure&lt;WallabyOptions&gt;</c>, configuration binding, or
    /// <c>PostConfigure</c>, and is validated as non-empty on first resolution.
    /// </summary>
    public WallabyBuilder UseConnectionString(string connectionString)
    {
        _configuration.OptionsActions.Add(options => options.ConnectionString = connectionString);
        return this;
    }

    /// <summary>
    /// Register a storage provider that derives a capture model and leases enrichment sessions.
    /// Called by provider packages' registration extensions (e.g. <c>UseEntityFrameworkCore&lt;TContext&gt;()</c>
    /// from Wallaby.EntityFrameworkCore); consumers normally never call it directly. A provider is required
    /// whenever Wallaby streams (any sink or <c>Map&lt;T&gt;()</c>) and to
    /// resolve <c>AddExternalSlot(...).ForEntity&lt;T&gt;()</c> table declarations; omit it for a
    /// provision-only worker that declares external slots by table name only. Multiple providers may be
    /// registered (their capture plans merge onto one slot/publication); names must be unique.
    /// </summary>
    public WallabyBuilder UseProvider(WallabyProviderRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        if (_configuration.Providers.Any(p => p.Name == registration.Name))
        {
            throw new WallabyConfigurationException(
                $"A storage provider named '{registration.Name}' is already registered. Provider names must be unique.");
        }
        _configuration.Providers.Add(registration);
        return this;
    }

    /// <summary>
    /// Override the named provider's enrichment sessions with a scope-key-aware provider (e.g.
    /// context-per-tenant). Called by provider packages' scoped-context extensions (e.g.
    /// <c>UseScopedDbContext</c>); used by mappings that declare <c>ScopedBy(...)</c>. The provider must be
    /// registered (via <see cref="UseProvider"/>) before this is called.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public WallabyBuilder UseScopedEnrichmentSessions(
        string providerName, Func<IServiceProvider, IEnrichmentSessionProvider> factory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(factory);
        var registration = _configuration.Providers.FirstOrDefault(p => p.Name == providerName)
            ?? throw new WallabyConfigurationException(
                $"UseScopedEnrichmentSessions(\"{providerName}\", ...) requires that provider to be registered first " +
                "(e.g. call UseEntityFrameworkCore<TContext>() before UseScopedDbContext(...)).");
        registration.ScopedEnrichmentSessions = factory;
        return this;
    }

    /// <summary>Register a sink instance (keyed by its <see cref="ISink.Name"/>).</summary>
    public WallabyBuilder AddSink(ISink sink)
    {
        _configuration.Sinks.Add(new SinkRegistration { Name = sink.Name, Factory = _ => sink });
        return this;
    }

    /// <summary>Register a sink resolved from the container.</summary>
    public WallabyBuilder AddSink(string name, Func<IServiceProvider, ISink> factory)
    {
        _configuration.Sinks.Add(new SinkRegistration { Name = name, Factory = factory });
        return this;
    }

    /// <summary>Register an in-process delegate sink.</summary>
    public WallabyBuilder AddDelegateSink(string name, Func<SinkBatch, CancellationToken, Task<DeliveryResult>> handler)
        => AddSink(new DelegateSink(name, handler));

    /// <summary>
    /// Provision an additional pgoutput publication + logical replication slot for the declared tables.
    /// Wallaby creates it and reconciles its table set on every startup, but never consumes it — so a
    /// third-party CDC tool (e.g. an ELT) can read from it independently. Wallaby never drops these slots;
    /// remove a no-longer-needed slot/publication manually (it pins WAL until then).
    /// </summary>
    public WallabyBuilder AddExternalSlot(string slotName, Action<ExternalSlotBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(slotName);
        ArgumentNullException.ThrowIfNull(configure);
        var registration = new ExternalSlotRegistration { SlotName = slotName };
        configure(new ExternalSlotBuilder(registration));
        _configuration.ExternalSlots.Add(registration);
        return this;
    }

    /// <summary>
    /// Spill pgoutput v2 streamed (large) transactions to local disk instead of the default database backend.
    /// Lowest source-DB impact and the truest memory bound, but needs a writable <paramref name="directory"/> —
    /// defaults to a per-slot folder under the OS temp path; mount a writable volume when the container's root
    /// filesystem is read-only.
    /// </summary>
    public WallabyBuilder SpillToDisk(string? directory = null)
        => UseTransactionSpill(ctx => new FileTransactionSpill(
            directory ?? Path.Combine(Path.GetTempPath(), "wallaby", ctx.SlotName)));

    /// <summary>
    /// Spill pgoutput v2 streamed (large) transactions to a <c>wallaby.stream_buffer</c> UNLOGGED table on the
    /// source database. This is the default — disk-free and zero-config (works wherever Wallaby connects), at the
    /// cost of extra source-DB I/O during a huge transaction. Use <see cref="SpillToDisk"/> to avoid that I/O when
    /// a writable path is available.
    /// </summary>
    public WallabyBuilder SpillToDatabase()
        => UseTransactionSpill(ctx => new PostgresUnloggedTableSpill(ctx.DataSource, ctx.SlotName));

    /// <summary>
    /// Supply a custom <see cref="ITransactionSpill"/> backend for pgoutput v2 streamed (large) transactions —
    /// e.g. an object store or cache. The <paramref name="factory"/> is invoked once per leader session with a
    /// <see cref="SpillContext"/> (the source data source, slot name, and service provider), so it may resolve
    /// its own dependencies and should return a fresh instance each call (the runtime disposes it at session end).
    /// Note that only a backend spilling to durable/external storage actually bounds memory; an in-RAM store just
    /// relocates it. Overrides <see cref="SpillToDisk"/>/<see cref="SpillToDatabase"/>; the default is the database.
    /// </summary>
    public WallabyBuilder UseTransactionSpill(Func<SpillContext, ITransactionSpill> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _configuration.SpillFactory = factory;
        return this;
    }

    /// <summary>Map an entity to a sink/destination via a transform.</summary>
    public EntityMapBuilder<TEntity> Map<TEntity>() where TEntity : class
    {
        var registration = new MappingRegistration { EntityClrType = typeof(TEntity) };
        _configuration.Mappings[typeof(TEntity)] = registration;
        return new EntityMapBuilder<TEntity>(registration);
    }

    internal WallabyConfiguration Build()
    {
        // Structural validation only — option VALUES (the connection string, slot/publication names, sizes,
        // intervals) are not final until the options pipeline runs (Configure/binding/PostConfigure may still
        // supply or change them), so those checks live in WallabyOptionsValidator and surface on first WallabyOptions
        // resolution.

        // Capturing (any sink or Map<>()) requires a provider + a sink. Without either, Wallaby runs in
        // provision-only mode: it just provisions the declared external slots (no primary slot, no
        // streaming), so neither a provider nor a sink is required.
        if (_configuration.CaptureIntended)
        {
            if (_configuration.Providers.Count == 0)
            {
                throw new WallabyConfigurationException(
                    "Capturing requires a storage provider. Register one with " +
                    "UseEntityFrameworkCore<TContext>() (from Wallaby.EntityFrameworkCore).");
            }
            if (_configuration.Sinks.Count == 0)
            {
                throw new WallabyConfigurationException(
                    "At least one sink must be registered when capturing (e.g. AddMeilisearchSink/AddDelegateSink).");
            }
        }

        foreach (var mapping in _configuration.Mappings.Values)
        {
            if (string.IsNullOrEmpty(mapping.SinkName))
            {
                throw new WallabyConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>() is missing a sink. Call .ToSink(\"<name>\", ...).");
            }
            if (mapping.TransformFactory is null)
            {
                throw new WallabyConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>() is missing a transform. Call .UsingTransform(...).");
            }
            if (mapping.DestinationSelector is not null && mapping.ScopeKeySelector is null)
            {
                throw new WallabyConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>().ScopedDestination(...) requires .ScopedBy(...) to provide the scope key.");
            }
            if (mapping.ExplicitProviderName is not null && mapping.TransformProviderName is not null &&
                mapping.ExplicitProviderName != mapping.TransformProviderName)
            {
                throw new WallabyConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>() is pinned to provider '{mapping.ExplicitProviderName}' via " +
                    $"FromProvider(...), but its transform is typed for provider '{mapping.TransformProviderName}'. " +
                    "Use that provider's UsingTransform overload or align the FromProvider name.");
            }
            if (mapping.ProviderName is not null &&
                !_configuration.Providers.Any(p => p.Name == mapping.ProviderName))
            {
                throw new WallabyConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>() is pinned to provider '{mapping.ProviderName}', which is not " +
                    $"registered. Registered providers: {DescribeProviders()}.");
            }
            if (mapping.ScopeKeySelector is not null && mapping.DestinationSelector is null &&
                !_configuration.Providers.Any(p => p.ScopedEnrichmentSessions is not null))
            {
                throw new WallabyConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>().ScopedBy(...) has no effect: add .ScopedDestination(...) or register UseScopedContext(...).");
            }
            // Scoped destinations must resolve the scope key on deletes too, which needs full old-row values.
            if (mapping.DestinationSelector is not null)
            {
                _configuration.RequiresFullReplicaIdentity.Add(mapping.EntityClrType);
            }
        }

        // ForEntity<T>() resolves against a provider's model, so it needs a declared provider. ForTable(...) does not.
        if (_configuration.Providers.Count == 0 &&
            _configuration.ExternalSlots.Any(e => e.EntityTypes.Count > 0))
        {
            throw new WallabyConfigurationException(
                "AddExternalSlot(...).ForEntity<T>() requires a storage provider to resolve the table. " +
                "Register one with UseEntityFrameworkCore<TContext>() or declare the table by name via ForTable(...).");
        }

        // External slots: names must be distinct from each other, and each must declare at least one table
        // (a pgoutput publication needs tables). Collisions with the PRIMARY slot/publication are checked by
        // WallabyOptionsValidator, since those names are not final until the options pipeline runs. The default
        // publication name here must match ExternalSlotResolver.
        var slotNames = new HashSet<string>(StringComparer.Ordinal);
        var publicationNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var external in _configuration.ExternalSlots)
        {
            if (external.TableNames.Count == 0 && external.EntityTypes.Count == 0)
            {
                throw new WallabyConfigurationException(
                    $"AddExternalSlot(\"{external.SlotName}\") declares no tables. Add at least one via ForTable(...) or ForEntity<T>().");
            }
            if (!slotNames.Add(external.SlotName))
            {
                throw new WallabyConfigurationException(
                    $"External slot name '{external.SlotName}' collides with another external slot.");
            }
            if (!publicationNames.Add(external.ResolvedPublicationName))
            {
                throw new WallabyConfigurationException(
                    $"External publication name '{external.ResolvedPublicationName}' collides with another external slot.");
            }
        }

        return _configuration;
    }

    private string DescribeProviders()
        => string.Join(", ", _configuration.Providers.Select(p => $"'{p.Name}'"));
}
