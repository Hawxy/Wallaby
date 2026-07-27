using Wallaby.Internal.Backfill;

namespace Wallaby.Tests.Unit;

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
    public void Cursor_round_trips_bytea_dateonly_timeonly_and_timespan()
    {
        var bytes = new byte[] { 1, 2, 255 };
        var date = new DateOnly(2026, 7, 25);
        var time = new TimeOnly(23, 59, 58, 123); // sub-minute precision must survive
        var span = new TimeSpan(1, 2, 3, 4, 5);
        string[] pk = ["a", "b", "c", "d"];

        var json = KeysetCodec.SerializeCursor([bytes, date, time, span], pk);

        KeysetCodec.TryDeserializeCursor(
                json, pk, [typeof(byte[]), typeof(DateOnly), typeof(TimeOnly), typeof(TimeSpan)], out var cursor)
            .ShouldBeTrue();

        cursor.ShouldNotBeNull();
        cursor[0].ShouldBeOfType<byte[]>().ShouldBe(bytes);
        cursor[1].ShouldBe(date);
        cursor[2].ShouldBe(time);
        cursor[3].ShouldBe(span);
    }

    [Test]
    public void An_unsupported_cursor_value_type_throws_at_serialize_time()
    {
        Should.Throw<NotSupportedException>(
            () => KeysetCodec.SerializeCursor([new System.Collections.BitArray(3)], ["a"]));
    }

    [Test]
    public void A_cursor_whose_values_cannot_be_coerced_is_rejected()
    {
        KeysetCodec.TryDeserializeCursor(
                """{"v":1,"pk":["tenant_id","order_id"],"cur":["nope","x"]}""",
                Pk, [typeof(int), typeof(string)], out _)
            .ShouldBeFalse();

        KeysetCodec.TryDeserializeScopedCursor(
                """{"v":1,"b":1,"pk":["tenant_id","order_id"],"cur":["nope","x"]}""",
                Pk, [typeof(int), typeof(string)], out _, out _)
            .ShouldBeFalse();
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
    public void Scoped_cursor_round_trips_its_batch_index()
    {
        var json = KeysetCodec.SerializeScopedCursor(3, [42, "abc"], Pk);

        KeysetCodec.TryDeserializeScopedCursor(json, Pk, [typeof(int), typeof(string)], out var batch, out var cursor)
            .ShouldBeTrue();

        batch.ShouldBe(3);
        cursor.ShouldNotBeNull();
        cursor[0].ShouldBe(42);
        cursor[1].ShouldBe("abc");
    }

    [Test]
    public void A_scoped_cursor_at_batch_start_round_trips_with_a_null_cursor()
    {
        var json = KeysetCodec.SerializeScopedCursor(2, null, Pk);

        json.ShouldNotBeNull();
        KeysetCodec.TryDeserializeScopedCursor(json, Pk, [typeof(int), typeof(string)], out var batch, out var cursor)
            .ShouldBeTrue();

        batch.ShouldBe(2);
        cursor.ShouldBeNull();
    }

    [Test]
    public void A_legacy_cursor_deserializes_as_batch_zero()
    {
        var json = KeysetCodec.SerializeCursor([42, "abc"], Pk);

        KeysetCodec.TryDeserializeScopedCursor(json, Pk, [typeof(int), typeof(string)], out var batch, out var cursor)
            .ShouldBeTrue();

        batch.ShouldBe(0);
        cursor.ShouldNotBeNull();
        cursor[0].ShouldBe(42);
    }

    [Test]
    public void A_scoped_cursor_with_mismatched_pk_columns_is_rejected()
    {
        var json = KeysetCodec.SerializeScopedCursor(1, [42, "abc"], Pk);

        KeysetCodec.TryDeserializeScopedCursor(json, ["other", "columns"], [typeof(int), typeof(string)], out _, out _)
            .ShouldBeFalse();
    }

    [Test]
    public void Null_or_empty_json_is_a_fresh_scoped_start()
    {
        KeysetCodec.TryDeserializeScopedCursor(null, Pk, [typeof(int), typeof(int)], out var batch, out var cursor)
            .ShouldBeTrue();
        batch.ShouldBe(0);
        cursor.ShouldBeNull();
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
