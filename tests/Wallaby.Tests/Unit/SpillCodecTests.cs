using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Model;

namespace Wallaby.Tests.Unit;

/// <summary>
/// <see cref="SpillCodec"/> must round-trip a <see cref="RawChange"/> with full CLR-type fidelity — it's how a
/// spilled (streamed) transaction is reconstructed before materialization, so any drift here corrupts data.
/// </summary>
public class SpillCodecTests
{
    private static RawChange RoundTrip(RawChange change) => SpillCodec.Decode(SpillCodec.Encode(change));

    [Test]
    public void Round_trips_scalar_types_with_fidelity()
    {
        var guid = Guid.NewGuid();
        var utc = new DateTime(2024, 1, 2, 3, 4, 5, 678, DateTimeKind.Utc);
        var dto = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(5));

        var change = new RawChange
        {
            RelationId = 42,
            Schema = "public",
            TableName = "products",
            Action = ChangeAction.Update,
            NewValues =
            [
                new RawColumn { ColumnName = "bool", Value = true },
                new RawColumn { ColumnName = "i16", Value = (short)7 },
                new RawColumn { ColumnName = "i32", Value = 123 },
                new RawColumn { ColumnName = "i64", Value = 9_999_999_999L },
                new RawColumn { ColumnName = "dec", Value = 12.3400m },
                new RawColumn { ColumnName = "f64", Value = 3.141592653589793d },
                new RawColumn { ColumnName = "f32", Value = 1.5f },
                new RawColumn { ColumnName = "str", Value = "héllo, wörld" },
                new RawColumn { ColumnName = "char", Value = 'x' },
                new RawColumn { ColumnName = "guid", Value = guid },
                new RawColumn { ColumnName = "dt", Value = utc },
                new RawColumn { ColumnName = "dto", Value = dto },
                new RawColumn { ColumnName = "date", Value = new DateOnly(2024, 1, 2) },
                new RawColumn { ColumnName = "time", Value = new TimeOnly(3, 4, 5) },
                new RawColumn { ColumnName = "span", Value = TimeSpan.FromMinutes(90) },
                new RawColumn { ColumnName = "bytes", Value = new byte[] { 1, 2, 3, 255 } },
                new RawColumn { ColumnName = "nullcol", Value = null },
            ],
            OldValues = null,
        };

        var r = RoundTrip(change).NewValues;

        r[0].Value.ShouldBe(true);
        r[1].Value.ShouldBe((short)7);
        r[2].Value.ShouldBe(123);
        r[3].Value.ShouldBe(9_999_999_999L);
        r[4].Value.ShouldBe(12.3400m);
        r[5].Value.ShouldBe(3.141592653589793d);
        r[6].Value.ShouldBe(1.5f);
        r[7].Value.ShouldBe("héllo, wörld");
        r[8].Value.ShouldBe('x');
        r[9].Value.ShouldBe(guid);
        r[10].Value.ShouldBe(utc);
        ((DateTime)r[10].Value!).Kind.ShouldBe(DateTimeKind.Utc);
        r[11].Value.ShouldBe(dto);
        ((DateTimeOffset)r[11].Value!).Offset.ShouldBe(TimeSpan.FromHours(5));
        r[12].Value.ShouldBe(new DateOnly(2024, 1, 2));
        r[13].Value.ShouldBe(new TimeOnly(3, 4, 5));
        r[14].Value.ShouldBe(TimeSpan.FromMinutes(90));
        ((byte[])r[15].Value!).ShouldBe(new byte[] { 1, 2, 3, 255 }, ignoreOrder: true);
        r[16].Value.ShouldBeNull();
    }

    [Test]
    public void Round_trips_arrays_of_tagged_scalars()
    {
        var guids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        var change = new RawChange
        {
            RelationId = 1,
            Schema = "public",
            TableName = "t",
            Action = ChangeAction.Insert,
            NewValues =
            [
                new RawColumn { ColumnName = "tags", Value = new[] { "a", null, "c" } },
                new RawColumn { ColumnName = "nums", Value = new[] { 1, 2, 3 } },
                new RawColumn { ColumnName = "longs", Value = new[] { 9_999_999_999L } },
                new RawColumn { ColumnName = "flags", Value = new[] { true, false } },
                new RawColumn { ColumnName = "decs", Value = new[] { 1.50m, 2.25m } },
                new RawColumn { ColumnName = "ids", Value = guids },
                new RawColumn { ColumnName = "empty", Value = Array.Empty<string>() },
            ],
        };

        var r = RoundTrip(change).NewValues;

        ((string?[])r[0].Value!).ShouldBe(new[] { "a", null, "c" });
        ((int[])r[1].Value!).ShouldBe(new[] { 1, 2, 3 });
        ((long[])r[2].Value!).ShouldBe(new[] { 9_999_999_999L });
        ((bool[])r[3].Value!).ShouldBe(new[] { true, false });
        ((decimal[])r[4].Value!).ShouldBe(new[] { 1.50m, 2.25m });
        ((Guid[])r[5].Value!).ShouldBe(guids);
        ((string[])r[6].Value!).ShouldBeEmpty();
    }

    [Test]
    public void Round_trips_untagged_types_via_json_fallback()
    {
        var change = new RawChange
        {
            RelationId = 1,
            Schema = "public",
            TableName = "t",
            Action = ChangeAction.Insert,
            NewValues = [new RawColumn { ColumnName = "maybe_nums", Value = new int?[] { 1, null, 3 } }],
        };

        var r = RoundTrip(change).NewValues;

        ((int?[])r[0].Value!).ShouldBe(new int?[] { 1, null, 3 });
    }

    [Test]
    public void Round_trips_unchanged_toast_and_old_values()
    {
        var change = new RawChange
        {
            RelationId = 7,
            Schema = "sales",
            TableName = "orders",
            Action = ChangeAction.Delete,
            NewValues = [],
            OldValues =
            [
                new RawColumn { ColumnName = "id", Value = 5 },
                new RawColumn { ColumnName = "blob", IsUnchangedToast = true },
            ],
        };

        var round = RoundTrip(change);

        round.Action.ShouldBe(ChangeAction.Delete);
        round.Schema.ShouldBe("sales");
        round.NewValues.Count.ShouldBe(0);
        round.OldValues!.Count.ShouldBe(2);
        round.OldValues![0].Value.ShouldBe(5);
        round.OldValues![1].IsUnchangedToast.ShouldBeTrue();
        round.OldValues![1].Value.ShouldBeNull();
    }
}
