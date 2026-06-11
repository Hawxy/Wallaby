using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.Replication;
using Wallaby.Sinks;

namespace Wallaby.DependencyInjection;

/// <summary>Fluent configuration for a CDC instance.</summary>
public sealed class CdcBuilder
{
    private readonly CdcConfiguration _configuration = new();

    /// <summary>
    /// Configure options (slot/publication names, chunk size, auto-backfill, etc.). The action joins the
    /// standard options pipeline at the <c>AddWallaby</c> registration position, so it composes with
    /// <c>services.Configure&lt;CdcOptions&gt;()</c>/<c>PostConfigure</c> calls: earlier <c>Configure</c>
    /// registrations run before it, later ones and <c>PostConfigure</c> override it.
    /// </summary>
    public CdcBuilder ConfigureOptions(Action<CdcOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _configuration.OptionsActions.Add(configure);
        return this;
    }

    /// <summary>
    /// Postgres connection string used for replication, checkpoint storage, advisory locks, and backfill reads.
    /// Shorthand for <c>ConfigureOptions(o =&gt; o.ConnectionString = ...)</c> — like any option value it can
    /// also be supplied (or overridden) through <c>Configure&lt;CdcOptions&gt;</c>, configuration binding, or
    /// <c>PostConfigure</c>, and is validated as non-empty on first resolution.
    /// </summary>
    public CdcBuilder UseConnectionString(string connectionString)
    {
        _configuration.OptionsActions.Add(options => options.ConnectionString = connectionString);
        return this;
    }

    /// <summary>
    /// Declare the EF Core <see cref="DbContext"/> that drives capture. Required whenever Wallaby streams
    /// (any sink, <c>Map&lt;T&gt;()</c>, or <c>CaptureAllMappedTables()</c>) and to resolve
    /// <c>AddExternalSlot(...).ForEntity&lt;T&gt;()</c> table declarations. The consumer registers the context as
    /// usual — a scoped <c>AddDbContext&lt;TContext&gt;()</c> is sufficient (Wallaby uses an
    /// <see cref="IDbContextFactory{TContext}"/> if one is registered, otherwise a DI scope). Omit it entirely
    /// for a provision-only worker that declares external slots by table name only.
    /// </summary>
    public CdcBuilder UseContext<TContext>() where TContext : DbContext
    {
        _configuration.ModelAccessor = DbContextResolver.ReadModel<TContext>;
        _configuration.ContextLease = DbContextResolver.Lease<TContext>;
        return this;
    }

    /// <summary>Track every mapped entity in the model (opt-in; default is explicit declaration).</summary>
    public CdcBuilder CaptureAllMappedTables()
    {
        _configuration.CaptureAllMapped = true;
        return this;
    }

    /// <summary>Register a sink instance (keyed by its <see cref="ISink.Name"/>).</summary>
    public CdcBuilder AddSink(ISink sink)
    {
        _configuration.Sinks.Add(new SinkRegistration { Name = sink.Name, Factory = _ => sink });
        return this;
    }

    /// <summary>Register a sink resolved from the container.</summary>
    public CdcBuilder AddSink(string name, Func<IServiceProvider, ISink> factory)
    {
        _configuration.Sinks.Add(new SinkRegistration { Name = name, Factory = factory });
        return this;
    }

    /// <summary>Register an in-process delegate sink.</summary>
    public CdcBuilder AddDelegateSink(string name, Func<SinkBatch, CancellationToken, Task<DeliveryResult>> handler)
        => AddSink(new DelegateSink(name, handler));

    /// <summary>
    /// Build the enrichment <see cref="DbContext"/> handed to transforms from a row's scope key (e.g. tenant),
    /// e.g. by selecting a tenant connection string or a context carrying the tenant for global query filters.
    /// Used by mappings that declare <c>ScopedBy(...)</c>.
    /// </summary>
    public CdcBuilder UseScopedContext(Func<object?, IServiceProvider, DbContext> factory)
    {
        _configuration.ScopedContextFactory = factory;
        return this;
    }

    /// <summary>
    /// Provision an additional pgoutput publication + logical replication slot for the declared tables.
    /// Wallaby creates it and reconciles its table set on every startup, but never consumes it — so a
    /// third-party CDC tool (e.g. an ELT) can read from it independently. Wallaby never drops these slots;
    /// remove a no-longer-needed slot/publication manually (it pins WAL until then).
    /// </summary>
    public CdcBuilder AddExternalSlot(string slotName, Action<ExternalSlotBuilder> configure)
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
    public CdcBuilder SpillToDisk(string? directory = null)
        => UseTransactionSpill(ctx => new FileTransactionSpill(
            directory ?? Path.Combine(Path.GetTempPath(), "wallaby", ctx.SlotName)));

    /// <summary>
    /// Spill pgoutput v2 streamed (large) transactions to a <c>wallaby.stream_buffer</c> UNLOGGED table on the
    /// source database. This is the default — disk-free and zero-config (works wherever Wallaby connects), at the
    /// cost of extra source-DB I/O during a huge transaction. Use <see cref="SpillToDisk"/> to avoid that I/O when
    /// a writable path is available.
    /// </summary>
    public CdcBuilder SpillToDatabase()
        => UseTransactionSpill(ctx => new PostgresUnloggedTableSpill(ctx.DataSource, ctx.SlotName));

    /// <summary>
    /// Supply a custom <see cref="ITransactionSpill"/> backend for pgoutput v2 streamed (large) transactions —
    /// e.g. an object store or cache. The <paramref name="factory"/> is invoked once per leader session with a
    /// <see cref="SpillContext"/> (the source data source, slot name, and service provider), so it may resolve
    /// its own dependencies and should return a fresh instance each call (the runtime disposes it at session end).
    /// Note that only a backend spilling to durable/external storage actually bounds memory; an in-RAM store just
    /// relocates it. Overrides <see cref="SpillToDisk"/>/<see cref="SpillToDatabase"/>; the default is the database.
    /// </summary>
    public CdcBuilder UseTransactionSpill(Func<SpillContext, ITransactionSpill> factory)
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

    internal CdcConfiguration Build()
    {
        // Structural validation only — option VALUES (the connection string, slot/publication names, sizes,
        // intervals) are not final until the options pipeline runs (Configure/binding/PostConfigure may still
        // supply or change them), so those checks live in CdcOptionsValidator and surface on first CdcOptions
        // resolution.

        // Capturing (any sink, Map<>(), or CaptureAllMappedTables()) requires a context + a sink. Without
        // any of these, Wallaby runs in provision-only mode: it just provisions the declared external slots
        // (no primary slot, no streaming), so neither a context nor a sink is required.
        if (_configuration.CaptureIntended)
        {
            if (_configuration.ModelAccessor is null)
            {
                throw new CdcConfigurationException(
                    "Capturing requires a DbContext. Declare it with UseContext<TContext>().");
            }
            if (_configuration.Sinks.Count == 0)
            {
                throw new CdcConfigurationException(
                    "At least one sink must be registered when capturing (e.g. AddMeilisearchSink/AddDelegateSink).");
            }
        }

        foreach (var mapping in _configuration.Mappings.Values)
        {
            if (string.IsNullOrEmpty(mapping.SinkName))
            {
                throw new CdcConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>() is missing a sink. Call .ToSink(\"<name>\", ...).");
            }
            if (mapping.TransformFactory is null)
            {
                throw new CdcConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>() is missing a transform. Call .UsingTransform(...).");
            }
            if (mapping.DestinationSelector is not null && mapping.ScopeKeySelector is null)
            {
                throw new CdcConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>().ScopedDestination(...) requires .ScopedBy(...) to provide the scope key.");
            }
            if (mapping.ScopeKeySelector is not null && mapping.DestinationSelector is null && _configuration.ScopedContextFactory is null)
            {
                throw new CdcConfigurationException(
                    $"Map<{mapping.EntityClrType.Name}>().ScopedBy(...) has no effect: add .ScopedDestination(...) or register UseScopedContext(...).");
            }
            // Scoped destinations must resolve the scope key on deletes too, which needs full old-row values.
            if (mapping.DestinationSelector is not null)
            {
                _configuration.RequiresFullReplicaIdentity.Add(mapping.EntityClrType);
            }
        }

        // ForEntity<T>() resolves against the EF model, so it needs a declared context. ForTable(...) does not.
        if (_configuration.ModelAccessor is null &&
            _configuration.ExternalSlots.Any(e => e.EntityTypes.Count > 0))
        {
            throw new CdcConfigurationException(
                "AddExternalSlot(...).ForEntity<T>() requires a DbContext to resolve the table. " +
                "Declare one with UseContext<TContext>() or declare the table by name via ForTable(...).");
        }

        // External slots: names must be distinct from each other, and each must declare at least one table
        // (a pgoutput publication needs tables). Collisions with the PRIMARY slot/publication are checked by
        // CdcOptionsValidator, since those names are not final until the options pipeline runs. The default
        // publication name here must match ExternalSlotResolver.
        var slotNames = new HashSet<string>(StringComparer.Ordinal);
        var publicationNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var external in _configuration.ExternalSlots)
        {
            if (external.TableNames.Count == 0 && external.EntityTypes.Count == 0)
            {
                throw new CdcConfigurationException(
                    $"AddExternalSlot(\"{external.SlotName}\") declares no tables. Add at least one via ForTable(...) or ForEntity<T>().");
            }
            if (!slotNames.Add(external.SlotName))
            {
                throw new CdcConfigurationException(
                    $"External slot name '{external.SlotName}' collides with another external slot.");
            }
            if (!publicationNames.Add(external.ResolvedPublicationName))
            {
                throw new CdcConfigurationException(
                    $"External publication name '{external.ResolvedPublicationName}' collides with another external slot.");
            }
        }

        return _configuration;
    }
}
