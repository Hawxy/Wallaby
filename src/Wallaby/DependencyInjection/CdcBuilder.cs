using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.Sinks;

namespace Wallaby.DependencyInjection;

/// <summary>Fluent configuration for a CDC instance.</summary>
public sealed class CdcBuilder
{
    private readonly CdcConfiguration _configuration = new() { Options = new CdcOptions() };

    /// <summary>Configure options (slot/publication names, chunk size, auto-backfill, etc.).</summary>
    public CdcBuilder ConfigureOptions(Action<CdcOptions> configure)
    {
        configure(_configuration.Options);
        return this;
    }

    /// <summary>Postgres connection string used for replication, checkpoint storage, advisory locks, and backfill reads.</summary>
    public CdcBuilder UseConnectionString(string connectionString)
    {
        _configuration.Options.ConnectionString = connectionString;
        return this;
    }

    /// <summary>
    /// Declare the EF Core <see cref="DbContext"/> that drives capture. Required whenever Wallaby streams
    /// (any sink, <c>Map&lt;T&gt;()</c>, or <c>CaptureAllMappedTables()</c>) and to resolve
    /// <c>AddExternalSlot(...).ForEntity&lt;T&gt;()</c> table declarations. The consumer must also register an
    /// <see cref="IDbContextFactory{TContext}"/> (e.g. via <c>AddDbContextFactory&lt;TContext&gt;</c>). Omit it
    /// for a provision-only worker that declares external slots by table name only.
    /// </summary>
    public CdcBuilder UseContext<TContext>() where TContext : DbContext
    {
        _configuration.ModelAccessor = sp =>
        {
            using var context = sp.GetRequiredService<IDbContextFactory<TContext>>().CreateDbContext();
            return context.Model;
        };
        _configuration.RegisterCaptureRuntime =
            services => CdcServiceCollectionExtensions.RegisterCaptureRuntime<TContext>(services);
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

    /// <summary>Map an entity to a sink/destination via a transform.</summary>
    public EntityMapBuilder<TEntity> Map<TEntity>() where TEntity : class
    {
        var registration = new MappingRegistration { EntityClrType = typeof(TEntity) };
        _configuration.Mappings[typeof(TEntity)] = registration;
        return new EntityMapBuilder<TEntity>(registration);
    }

    internal CdcConfiguration Build()
    {
        var options = _configuration.Options;
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new CdcConfigurationException("A connection string must be supplied via UseConnectionString(...).");
        }
        if (string.IsNullOrWhiteSpace(options.SlotName) || string.IsNullOrWhiteSpace(options.PublicationName))
        {
            throw new CdcConfigurationException("SlotName and PublicationName must be non-empty.");
        }
        if (options.ChunkSize <= 0)
        {
            throw new CdcConfigurationException("ChunkSize must be greater than zero.");
        }
        if (options.MaxBatchSize <= 0)
        {
            throw new CdcConfigurationException("MaxBatchSize must be greater than zero.");
        }
        if (options.KeepaliveInterval <= TimeSpan.Zero)
        {
            throw new CdcConfigurationException("KeepaliveInterval must be greater than zero.");
        }
        if (options.LeaderHeartbeatInterval <= TimeSpan.Zero)
        {
            throw new CdcConfigurationException("LeaderHeartbeatInterval must be greater than zero.");
        }

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

        // External slots: names must be non-empty and distinct from each other (and from the primary
        // slot/publication when capturing — provision-only has no primary); each must declare at least one
        // table (a pgoutput publication needs tables). The default publication name here must match
        // ExternalSlotResolver.
        var slotNames = new HashSet<string>(StringComparer.Ordinal);
        var publicationNames = new HashSet<string>(StringComparer.Ordinal);
        if (_configuration.CaptureIntended)
        {
            slotNames.Add(options.SlotName);
            publicationNames.Add(options.PublicationName);
        }
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
                    $"External slot name '{external.SlotName}' collides with the primary slot or another external slot.");
            }
            var publication = string.IsNullOrWhiteSpace(external.PublicationName)
                ? $"{external.SlotName}_pub"
                : external.PublicationName;
            if (!publicationNames.Add(publication))
            {
                throw new CdcConfigurationException(
                    $"External publication name '{publication}' collides with the primary publication or another external slot.");
            }
        }

        return _configuration;
    }
}
