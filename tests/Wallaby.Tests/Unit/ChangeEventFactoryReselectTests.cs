using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Pipeline;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Tests.Unit;

/// <summary>
/// The factory's reselect recovery: an unavailable (unchanged TOAST) value heals by re-reading the
/// row, a vanished row's change is dropped, a failing re-read halts with annotated context, and the
/// path is bypassed when no reselector is supplied or the change cannot carry unchanged TOAST.
/// </summary>
public class ChangeEventFactoryReselectTests
{
    private const string ToastMessage = "Column 'Description' was not carried in the change.";

    /// <summary>Throws for changes whose Description column is flagged unchanged-TOAST, else materializes.</summary>
    private sealed class ToastAwareMaterializer : IRowMaterializer
    {
        public int Attempts;

        public bool TryMaterialize(RawChange change, [NotNullWhen(true)] out MaterializedRow? row)
        {
            Attempts++;
            foreach (var column in change.NewValues)
            {
                if (column.IsUnchangedToast)
                {
                    throw new UnavailableValueException(change.Schema, change.TableName, column.ColumnName, ToastMessage);
                }
            }

            var record = change.NewValues.ToDictionary(c => c.ColumnName, c => c.Value);
            row = new MaterializedRow(change.Action, Entity: null, record, Changes: null, [record["Id"]!], typeof(object));
            return true;
        }
    }

    private sealed class StubReselector(Func<RawChange, RawChange?> reselect) : IRowReselector
    {
        public int Calls;

        public ValueTask<RawChange?> ReselectAsync(RawChange change, CancellationToken ct)
        {
            Calls++;
            return ValueTask.FromResult(reselect(change));
        }
    }

    private static RawChange ToastedUpdate(ChangeAction action = ChangeAction.Update) => new()
    {
        RelationId = 1,
        Schema = "public",
        TableName = "products",
        Action = action,
        NewValues =
        [
            new RawColumn { ColumnName = "Id", Value = 42 },
            new RawColumn { ColumnName = "Description", IsUnchangedToast = true },
        ],
        CommitLsn = 42,
        CommitTimestamp = DateTimeOffset.UnixEpoch,
        CommitIdx = 7,
    };

    private static RawChange Healed(RawChange change) => change with
    {
        NewValues =
        [
            new RawColumn { ColumnName = "Id", Value = 42 },
            new RawColumn { ColumnName = "Description", Value = "a widget" },
        ],
    };

    [Test]
    public async Task An_unavailable_value_heals_by_reselect_preserving_commit_metadata()
    {
        var materializer = new ToastAwareMaterializer();
        var reselector = new StubReselector(Healed);
        var instrumentation = new WallabyInstrumentation();
        using var reselected = new MetricCollector<long>(instrumentation.Meter, "wallaby.changes.reselected");
        var factory = new ChangeEventFactory(materializer, reselector, instrumentation: instrumentation);

        var ev = await factory.CreateAsync(ToastedUpdate(), CancellationToken.None);

        ev.ShouldNotBeNull();
        ev!.Action.ShouldBe(ChangeAction.Update);
        ev.Record["Description"].ShouldBe("a widget");
        ev.Metadata.CommitLsn.ShouldBe(42UL);
        ev.Metadata.CommitIdx.ShouldBe(7);
        ev.Metadata.IsBackfill.ShouldBeFalse();
        reselector.Calls.ShouldBe(1);
        materializer.Attempts.ShouldBe(2);

        var measurement = reselected.GetMeasurementSnapshot().ShouldHaveSingleItem();
        measurement.Value.ShouldBe(1);
        measurement.Tags.GetValueOrDefault("wallaby.reselect.outcome").ShouldBe("healed");
        measurement.Tags.GetValueOrDefault("wallaby.table").ShouldBe("public.products");
    }

    [Test]
    public async Task A_vanished_row_drops_the_change()
    {
        var instrumentation = new WallabyInstrumentation();
        using var reselected = new MetricCollector<long>(instrumentation.Meter, "wallaby.changes.reselected");
        var factory = new ChangeEventFactory(
            new ToastAwareMaterializer(), new StubReselector(_ => null), instrumentation: instrumentation);

        var ev = await factory.CreateAsync(ToastedUpdate(), CancellationToken.None);

        ev.ShouldBeNull();
        reselected.GetMeasurementSnapshot().ShouldHaveSingleItem()
            .Tags.GetValueOrDefault("wallaby.reselect.outcome").ShouldBe("row_gone");
    }

    [Test]
    public async Task A_failing_reselect_halts_with_annotated_context()
    {
        var inner = new TimeoutException("connection timed out");
        var factory = new ChangeEventFactory(
            new ToastAwareMaterializer(), new StubReselector(_ => throw inner));

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await factory.CreateAsync(ToastedUpdate(), CancellationToken.None));

        ex.Message.ShouldContain("public.products");
        ex.Message.ShouldContain("change #7");
        ex.Message.ShouldContain("column 'Description'");
        ex.InnerException.ShouldBeSameAs(inner);
    }

    [Test]
    public async Task Repeated_heals_on_one_table_log_once_then_roll_up()
    {
        var logger = new Microsoft.Extensions.Logging.Testing.FakeLogger();
        var factory = new ChangeEventFactory(new ToastAwareMaterializer(), new StubReselector(Healed), logger);

        // Three heals inside one rollup interval: the first logs, the rest are suppressed.
        for (var i = 0; i < 3; i++)
        {
            (await factory.CreateAsync(ToastedUpdate(), CancellationToken.None)).ShouldNotBeNull();
        }

        var warnings = logger.Collector.GetSnapshot();
        warnings.Count.ShouldBe(1);
        warnings[0].Message.ShouldContain("public.products");
        warnings[0].Message.ShouldContain("REPLICA IDENTITY FULL");
    }

    [Test]
    public async Task Without_a_reselector_the_change_is_a_poison_change()
    {
        var factory = new ChangeEventFactory(new ToastAwareMaterializer());

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await factory.CreateAsync(ToastedUpdate(), CancellationToken.None));

        ex.Message.ShouldContain("public.products");
        ex.Message.ShouldContain("change #7");
        ex.InnerException.ShouldBeOfType<UnavailableValueException>();
    }

    [Test]
    public async Task A_read_action_change_never_reselects()
    {
        var reselector = new StubReselector(Healed);
        var factory = new ChangeEventFactory(new ToastAwareMaterializer(), reselector);

        await Should.ThrowAsync<InvalidOperationException>(
            async () => await factory.CreateAsync(ToastedUpdate(ChangeAction.Read), CancellationToken.None));

        reselector.Calls.ShouldBe(0);
    }
}
