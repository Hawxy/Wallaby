using NpgsqlTypes;
using Wallaby.Abstractions;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Composes a <see cref="ChangeEvent"/> from a decoded <see cref="RawChange"/> by materializing it and
/// attaching commit/source metadata. Returns null when the change's table is not part of the model.
/// <para>
/// A materialization <em>failure</em> (e.g. a bad value/conversion or a missing key) is a poison change that
/// always halts the pipeline; it is rethrown annotated with the change's table and commit position.
/// </para>
/// </summary>
internal sealed class ChangeEventFactory(IRowMaterializer materializer)
{
    public ChangeEvent? Create(RawChange change)
    {
        bool materialized;
        MaterializedRow? row;
        try
        {
            materialized = materializer.TryMaterialize(change, out row);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"Materialization failed for a {change.Action} on {change.QualifiedName} " +
                $"(commit {new NpgsqlLogSequenceNumber(change.CommitLsn)}, change #{change.CommitIdx}): {ex.Message}", ex);
        }

        if (!materialized || row is null)
        {
            return null; // table not part of the model; benign skip
        }

        var metadata = new ChangeMetadata(
            change.Schema,
            change.TableName,
            row.Action,
            change.CommitTimestamp,
            change.CommitLsn,
            change.CommitIdx,
            IsBackfill: change.Action == ChangeAction.Read,
            change.BackfillRunId);

        return new ChangeEvent(
            row.Action,
            metadata,
            row.Entity,
            row.Record,
            row.Changes,
            row.PrimaryKey)
        {
            EntityClrType = row.EntityClrType,
        };
    }
}
