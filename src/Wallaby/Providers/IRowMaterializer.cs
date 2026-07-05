using System.Diagnostics.CodeAnalysis;
using Wallaby.Model;

namespace Wallaby.Providers;

/// <summary>
/// The storage provider's materialization seam: turns decoded <see cref="RawChange"/>s into materialized
/// CLR entities using the provider's model metadata. Built once per <see cref="CapturePlan"/>; providers
/// should precompute and cache any per-table plans internally.
/// </summary>
public interface IRowMaterializer
{
    /// <summary>
    /// Materialize a decoded change. Returns false when the change's table is not part of the model
    /// (a benign skip). A materialization <em>failure</em> (bad value/conversion, missing key) must throw —
    /// it is a poison change that halts the pipeline.
    /// </summary>
    bool TryMaterialize(RawChange change, [NotNullWhen(true)] out MaterializedRow? row);
}
