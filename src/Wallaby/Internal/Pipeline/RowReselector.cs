using Npgsql;
using Wallaby.Internal.Backfill;
using Wallaby.Model;

namespace Wallaby.Internal.Pipeline;

/// <summary>Re-reads a change's row by primary key to recover values pgoutput omitted (unchanged TOAST).</summary>
internal interface IRowReselector
{
    /// <summary>The complete change re-read from current row state, or null when the row no longer exists.</summary>
    ValueTask<RawChange?> ReselectAsync(RawChange change, CancellationToken ct);
}

/// <summary>
/// Default <see cref="IRowReselector"/> over the primary connection. The re-read selects only the
/// table's captured columns and returns current row state, not commit-time state; later updates to
/// the row are themselves in the stream, so sinks converge forward.
/// </summary>
internal sealed class RowReselector(NpgsqlDataSource dataSource, WallabyModel model) : IRowReselector
{
    public async ValueTask<RawChange?> ReselectAsync(RawChange change, CancellationToken ct)
    {
        var table = model.FindByRelation(change.Schema, change.TableName)
            ?? throw new InvalidOperationException(
                $"Cannot reselect a change for '{change.QualifiedName}': the table is not part of the model.");

        var pkColumns = new string[table.PrimaryKey.Count];
        var pkValues = new object?[table.PrimaryKey.Count];
        for (var i = 0; i < table.PrimaryKey.Count; i++)
        {
            var columnName = table.PrimaryKey[i].ColumnName;
            var column = FindColumn(change.NewValues, columnName);
            if (column is null or { IsUnchangedToast: true } or { Value: null })
            {
                throw new InvalidOperationException(
                    $"Cannot reselect a change for '{change.QualifiedName}': primary key column " +
                    $"'{columnName}' was not carried in the change.");
            }
            pkColumns[i] = columnName;
            pkValues[i] = column.Value;
        }

        // One tuple always yields exactly one filter (composite splitting only kicks in past the
        // parameter budget). Future optimization: memo reselected rows per batch and share one
        // connection across a batch's reselects.
        var filter = KeysetFilter.ForLookup(pkColumns, [pkValues])[0];
        var pager = new KeysetPager(table, filter);

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var chunk = await pager.ReadChunkAsync(connection, cursor: null, limit: 1, ct);
        if (chunk.Rows.Count == 0)
        {
            return null; // row no longer exists; its delete follows later in the stream
        }

        return new RawChange
        {
            RelationId = change.RelationId,
            Schema = change.Schema,
            TableName = change.TableName,
            Action = change.Action,
            NewValues = chunk.Rows[0].NewValues,
            OldValues = change.OldValues,
            BackfillRunId = change.BackfillRunId,
            CommitLsn = change.CommitLsn,
            CommitTimestamp = change.CommitTimestamp,
            CommitIdx = change.CommitIdx,
        };
    }

    private static RawColumn? FindColumn(IReadOnlyList<RawColumn> values, string columnName)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (values[i].ColumnName == columnName)
            {
                return values[i];
            }
        }
        return null;
    }
}
