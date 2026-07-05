using System.Diagnostics.CodeAnalysis;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Dispatches materialization to the owning provider's materializer by the change's (schema, table).
/// Built over the merged multi-provider model; a table no provider captures is a benign skip, matching
/// the single-materializer contract.
/// </summary>
internal sealed class CompositeRowMaterializer : IRowMaterializer
{
    private readonly Dictionary<(string Schema, string Table), IRowMaterializer> _byTable;

    /// <summary>Build the dispatch map from each provider's capture plan (tables are already conflict-checked).</summary>
    public CompositeRowMaterializer(IEnumerable<CapturePlan> plans)
    {
        _byTable = new Dictionary<(string, string), IRowMaterializer>();
        foreach (var plan in plans)
        {
            foreach (var table in plan.Model.Tables)
            {
                _byTable[(table.Schema, table.TableName)] = plan.Materializer;
            }
        }
    }

    public bool TryMaterialize(RawChange change, [NotNullWhen(true)] out MaterializedRow? row)
    {
        if (_byTable.TryGetValue((change.Schema, change.TableName), out var materializer))
        {
            return materializer.TryMaterialize(change, out row);
        }
        row = null;
        return false;
    }
}
