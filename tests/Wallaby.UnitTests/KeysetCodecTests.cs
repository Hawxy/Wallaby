using Wallaby.Internal.Backfill;

namespace Wallaby.UnitTests;

public class KeysetCodecTests
{
    private static readonly string[] Pk = ["tenant_id", "order_id"];

    [Test]
    public void Cursor_round_trips_numbers_and_strings()
    {
        var json = KeysetCodec.SerializeCursor([42, "abc"], Pk);

        KeysetCodec.TryDeserializeCursor(json, Pk, [typeof(int), typeof(string)], out var cursor).ShouldBeTrue();

        cursor.ShouldNotBeNull();
        cursor[0].ShouldBe(42);
        cursor[1].ShouldBe("abc");
    }

    [Test]
    public void Cursor_round_trips_guid_datetime_decimal_bool_and_null()
    {
        var guid = Guid.NewGuid();
        var at = new DateTimeOffset(2026, 7, 5, 12, 30, 15, TimeSpan.Zero);
        string[] pk = ["a", "b", "c", "d", "e"];

        var json = KeysetCodec.SerializeCursor([guid, at, 1.5m, true, null], pk);

        KeysetCodec.TryDeserializeCursor(
                json, pk, [typeof(Guid), typeof(DateTimeOffset), typeof(decimal), typeof(bool), typeof(string)],
                out var cursor)
            .ShouldBeTrue();

        cursor.ShouldNotBeNull();
        cursor[0].ShouldBe(guid);
        cursor[1].ShouldBe(at);
        cursor[2].ShouldBe(1.5m);
        cursor[3].ShouldBe(true);
        cursor[4].ShouldBeNull();
    }

    [Test]
    public void Null_or_empty_json_is_a_valid_fresh_start()
    {
        KeysetCodec.TryDeserializeCursor(null, Pk, [typeof(int), typeof(int)], out var cursor).ShouldBeTrue();
        cursor.ShouldBeNull();

        KeysetCodec.TryDeserializeCursor("", Pk, [typeof(int), typeof(int)], out cursor).ShouldBeTrue();
        cursor.ShouldBeNull();
    }

    [Test]
    public void Null_cursor_serializes_to_null()
    {
        KeysetCodec.SerializeCursor(null, Pk).ShouldBeNull();
    }

    [Test]
    public void Legacy_bare_array_is_rejected()
    {
        KeysetCodec.TryDeserializeCursor("[1,2]", Pk, [typeof(int), typeof(int)], out var cursor).ShouldBeFalse();
        cursor.ShouldBeNull();
    }

    [Test]
    public void Malformed_json_is_rejected()
    {
        KeysetCodec.TryDeserializeCursor("not json", Pk, [typeof(int), typeof(int)], out _).ShouldBeFalse();
        KeysetCodec.TryDeserializeCursor("null", Pk, [typeof(int), typeof(int)], out _).ShouldBeFalse();
    }

    [Test]
    public void Pk_name_mismatch_is_rejected()
    {
        var json = KeysetCodec.SerializeCursor([1, 2], ["tenant_id", "product_id"]);

        KeysetCodec.TryDeserializeCursor(json, Pk, [typeof(int), typeof(int)], out _).ShouldBeFalse();
    }

    [Test]
    public void Pk_order_mismatch_is_rejected()
    {
        var json = KeysetCodec.SerializeCursor([1, 2], ["order_id", "tenant_id"]);

        KeysetCodec.TryDeserializeCursor(json, Pk, [typeof(int), typeof(int)], out _).ShouldBeFalse();
    }

    [Test]
    public void Pk_arity_mismatch_is_rejected()
    {
        var json = KeysetCodec.SerializeCursor([1], ["tenant_id"]);

        KeysetCodec.TryDeserializeCursor(json, Pk, [typeof(int), typeof(int)], out _).ShouldBeFalse();
    }

    [Test]
    public void Unknown_version_is_rejected()
    {
        KeysetCodec.TryDeserializeCursor(
                """{"v":2,"pk":["tenant_id","order_id"],"cur":[1,2]}""", Pk, [typeof(int), typeof(int)], out _)
            .ShouldBeFalse();
    }

    [Test]
    public void Tuples_round_trip()
    {
        var json = KeysetCodec.SerializeTuples([[1, "x"], [2, null]]);

        var tuples = KeysetCodec.DeserializeTuples(json, [typeof(int), typeof(string)]);

        tuples.Count.ShouldBe(2);
        tuples[0][0].ShouldBe(1);
        tuples[0][1].ShouldBe("x");
        tuples[1][0].ShouldBe(2);
        tuples[1][1].ShouldBeNull();
    }
}
