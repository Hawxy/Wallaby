using EFCore.CDC.Abstractions;
using EFCore.CDC.Internal.Pipeline;
using Microsoft.EntityFrameworkCore;

namespace EFCore.CDC.DependencyInjection;

/// <summary>A registered sink: its name and a factory that resolves the instance from the container.</summary>
internal sealed class SinkRegistration
{
    public required string Name { get; init; }
    public required Func<IServiceProvider, ISink> Factory { get; init; }
}

/// <summary>A registered entity→sink mapping plus the factory for its transform invoker.</summary>
internal sealed class MappingRegistration
{
    public required Type EntityClrType { get; init; }
    public string SinkName { get; set; } = "";
    public string? Destination { get; set; }
    public string? BackfillVersion { get; set; }
    public Func<IServiceProvider, ITransformInvoker>? TransformFactory { get; set; }
    public Func<ChangeEvent, string>? DocumentIdSelector { get; set; }

    /// <summary>Per-row scope key (e.g. tenant id) for enrichment-context + destination scoping.</summary>
    public Func<ChangeEvent, object?>? ScopeKeySelector { get; set; }

    /// <summary>Per-scope-key destination (e.g. index-per-tenant); falls back to <see cref="Destination"/>.</summary>
    public Func<object?, string?>? DestinationSelector { get; set; }
}

/// <summary>The immutable result of the fluent builder, consumed by the runtime.</summary>
internal sealed class CdcConfiguration
{
    public required CdcOptions Options { get; init; }
    public bool CaptureAllMapped { get; set; }
    public List<Type> DeclaredEntities { get; } = [];
    public List<SinkRegistration> Sinks { get; } = [];
    public Dictionary<Type, MappingRegistration> Mappings { get; } = [];
    public HashSet<Type> RequiresFullReplicaIdentity { get; } = [];

    /// <summary>Optional factory that builds the enrichment <see cref="DbContext"/> from a row's scope key.</summary>
    public Func<object?, IServiceProvider, DbContext>? ScopedContextFactory { get; set; }
}
