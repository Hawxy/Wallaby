using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Materialization;
using Wallaby.Model;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Composes a <see cref="ChangeEvent"/> from a decoded <see cref="RawChange"/> by materializing it and
/// attaching commit/source metadata. Returns null when the change's table is not part of the model.
/// <para>
/// A materialization <em>failure</em> (e.g. a bad value/conversion or a missing key) is a poison change:
/// under <c>DeadLetterPolicy.Halt</c> it stops the pipeline (retried from the last ack); under
/// <c>Skip</c> it is logged, counted, and dropped so one bad row can't wedge the stream.
/// </para>
/// </summary>
internal sealed class ChangeEventFactory(
    EntityMaterializer materializer,
    bool skipFailedBatches = false,
    ILogger? logger = null,
    WallabyInstrumentation? instrumentation = null)
{
    private readonly ILogger _logger = logger ?? NullLogger.Instance;
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;

    public ChangeEvent? Create(RawChange change)
    {
        MaterializedRow row;
        try
        {
            if (!materializer.TryMaterialize(change, out row))
            {
                return null; // table not part of the model — benign skip
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (!skipFailedBatches)
            {
                throw;
            }
            _instr.RecordDeadLetter(WallabyInstrumentation.StageMaterialization);
            _logger.MaterializationDeadLettered(ex, change.Schema, change.TableName);
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

/// <summary>Source-generated log messages for <see cref="ChangeEventFactory"/>.</summary>
internal static partial class ChangeEventFactoryLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Dead-lettering a change for {Schema}.{Table}: materialization failed (DeadLetterPolicy=Skip).")]
    internal static partial void MaterializationDeadLettered(this ILogger logger, Exception ex, string schema, string table);
}
