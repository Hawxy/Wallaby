using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Model;

namespace EFCore.CDC.UnitTests;

/// <summary>
/// <see cref="SpillCodec"/> must round-trip a <see cref="RawChange"/> with full CLR-type fidelity — it's how a
/// spilled (streamed) transaction is reconstructed before materialization, so any drift here corrupts data.
/// </summary>
public class SpillCodecTests
{
    private static RawChange RoundTrip(RawChange change) => SpillCodec.Decode(SpillCodec.Encode(change));

    [Test]
    public async Task Round_trips_scalar_types_with_fidelity()
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

        await Assert.That(r[0].Value).IsEqualTo(true);
        await Assert.That(r[1].Value).IsEqualTo((short)7);
        await Assert.That(r[2].Value).IsEqualTo(123);
        await Assert.That(r[3].Value).IsEqualTo(9_999_999_999L);
        await Assert.That(r[4].Value).IsEqualTo(12.3400m);
        await Assert.That(r[5].Value).IsEqualTo(3.141592653589793d);
        await Assert.That(r[6].Value).IsEqualTo(1.5f);
        await Assert.That(r[7].Value).IsEqualTo("héllo, wörld");
        await Assert.That(r[8].Value).IsEqualTo('x');
        await Assert.That(r[9].Value).IsEqualTo(guid);
        await Assert.That(r[10].Value).IsEqualTo(utc);
        await Assert.That(((DateTime)r[10].Value!).Kind).IsEqualTo(DateTimeKind.Utc);
        await Assert.That(r[11].Value).IsEqualTo(dto);
        await Assert.That(((DateTimeOffset)r[11].Value!).Offset).IsEqualTo(TimeSpan.FromHours(5));
        await Assert.That(r[12].Value).IsEqualTo(new DateOnly(2024, 1, 2));
        await Assert.That(r[13].Value).IsEqualTo(new TimeOnly(3, 4, 5));
        await Assert.That(r[14].Value).IsEqualTo(TimeSpan.FromMinutes(90));
        await Assert.That((byte[])r[15].Value!).IsEquivalentTo(new byte[] { 1, 2, 3, 255 });
        await Assert.That(r[16].Value).IsNull();
    }

    [Test]
    public async Task Round_trips_arrays_via_json_fallback()
    {
        var change = new RawChange
        {
            RelationId = 1,
            Schema = "public",
            TableName = "t",
            Action = ChangeAction.Insert,
            NewValues =
            [
                new RawColumn { ColumnName = "tags", Value = new[] { "a", "b", "c" } },
                new RawColumn { ColumnName = "nums", Value = new[] { 1, 2, 3 } },
            ],
        };

        var r = RoundTrip(change).NewValues;

        await Assert.That((string[])r[0].Value!).IsEquivalentTo(new[] { "a", "b", "c" });
        await Assert.That((int[])r[1].Value!).IsEquivalentTo(new[] { 1, 2, 3 });
    }

    [Test]
    public async Task Round_trips_unchanged_toast_and_old_values()
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

        await Assert.That(round.Action).IsEqualTo(ChangeAction.Delete);
        await Assert.That(round.Schema).IsEqualTo("sales");
        await Assert.That(round.NewValues.Count).IsEqualTo(0);
        await Assert.That(round.OldValues!.Count).IsEqualTo(2);
        await Assert.That(round.OldValues![0].Value).IsEqualTo(5);
        await Assert.That(round.OldValues![1].IsUnchangedToast).IsTrue();
        await Assert.That(round.OldValues![1].Value).IsNull();
    }
}
