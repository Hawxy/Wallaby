using EFCore.CDC.Abstractions;
using EFCore.CDC.Internal.Materialization;
using EFCore.CDC.Model;

namespace EFCore.CDC.Internal.Pipeline;

/// <summary>
/// Composes a <see cref="ChangeEvent"/> from a decoded <see cref="RawChange"/> by materializing it and
/// attaching commit/source metadata. Returns null when the change's table is not part of the model.
/// </summary>
internal sealed class ChangeEventFactory(EntityMaterializer materializer)
{
    public ChangeEvent? Create(RawChange change)
    {
        if (!materializer.TryMaterialize(change, out var row))
        {
            return null;
        }

        var metadata = new ChangeMetadata(
            change.Schema,
            change.TableName,
            change.CommitTimestamp,
            change.CommitLsn,
            change.CommitIdx,
            IsBackfill: change.Action == ChangeAction.Read);

        return new ChangeEvent(
            change.Action,
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
