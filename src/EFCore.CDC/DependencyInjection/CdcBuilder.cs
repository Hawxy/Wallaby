using EFCore.CDC.Abstractions;
using EFCore.CDC.Sinks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.CDC.DependencyInjection;

/// <summary>Fluent configuration for a CDC instance. Obtained inside <c>AddCdc&lt;TContext&gt;(...)</c>.</summary>
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

    /// <summary>Track changes for a specific entity's table (without routing it anywhere by itself).</summary>
    public CdcBuilder Capture<TEntity>() where TEntity : class
    {
        AddDeclared(typeof(TEntity));
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

    /// <summary>Map an entity to a sink/destination via a transform.</summary>
    public EntityMapBuilder<TEntity> Map<TEntity>() where TEntity : class
    {
        var registration = new MappingRegistration { EntityClrType = typeof(TEntity) };
        _configuration.Mappings[typeof(TEntity)] = registration;
        AddDeclared(typeof(TEntity));
        return new EntityMapBuilder<TEntity>(registration);
    }

    private void AddDeclared(Type type)
    {
        if (!_configuration.DeclaredEntities.Contains(type))
        {
            _configuration.DeclaredEntities.Add(type);
        }
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
        if (_configuration.Sinks.Count == 0)
        {
            throw new CdcConfigurationException("At least one sink must be registered (e.g. AddMeilisearchSink/AddDelegateSink).");
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

        return _configuration;
    }
}
