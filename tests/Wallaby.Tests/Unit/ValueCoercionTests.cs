using Wallaby.Providers;

namespace Wallaby.Tests.Unit;

/// <summary>
/// <see cref="ValueCoercion.ToClr"/> bridges the CLR types the pgoutput decoder produces (Npgsql defaults:
/// date → DateOnly, time → TimeOnly, interval → TimeSpan) to mismatched entity property types. A raw value
/// it can't bridge reaches the property setter as the wrong type and poisons the change, so every supported
/// conversion is pinned here.
/// </summary>
public class ValueCoercionTests
{
    // ---- date/time bridges ----

    [Test]
    public void DateOnly_from_DateTime_and_string()
    {
        ValueCoercion.ToClr(new DateTime(2024, 1, 2, 13, 30, 0, DateTimeKind.Utc), typeof(DateOnly))
            .ShouldBe(new DateOnly(2024, 1, 2));
        ValueCoercion.ToClr("2024-01-02", typeof(DateOnly)).ShouldBe(new DateOnly(2024, 1, 2));
    }

    [Test]
    public void DateTime_from_DateOnly_is_midnight()
    {
        var result = ValueCoercion.ToClr(new DateOnly(2024, 1, 2), typeof(DateTime));

        result.ShouldBe(new DateTime(2024, 1, 2, 0, 0, 0));
    }

    [Test]
    public void DateTimeOffset_from_DateOnly_is_utc_midnight()
    {
        var result = ValueCoercion.ToClr(new DateOnly(2024, 1, 2), typeof(DateTimeOffset));

        result.ShouldBe(new DateTimeOffset(2024, 1, 2, 0, 0, 0, TimeSpan.Zero));
    }

    [Test]
    public void TimeOnly_from_TimeSpan_DateTime_and_string()
    {
        ValueCoercion.ToClr(new TimeSpan(3, 4, 5), typeof(TimeOnly)).ShouldBe(new TimeOnly(3, 4, 5));
        ValueCoercion.ToClr(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), typeof(TimeOnly))
            .ShouldBe(new TimeOnly(3, 4, 5));
        ValueCoercion.ToClr("03:04:05", typeof(TimeOnly)).ShouldBe(new TimeOnly(3, 4, 5));
    }

    [Test]
    public void TimeSpan_from_TimeOnly_and_string()
    {
        ValueCoercion.ToClr(new TimeOnly(3, 4, 5), typeof(TimeSpan)).ShouldBe(new TimeSpan(3, 4, 5));
        ValueCoercion.ToClr("03:04:05", typeof(TimeSpan)).ShouldBe(new TimeSpan(3, 4, 5));
    }

    [Test]
    public void Nullable_date_time_targets_unwrap()
    {
        ValueCoercion.ToClr(new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc), typeof(DateOnly?))
            .ShouldBe(new DateOnly(2024, 1, 2));
        ValueCoercion.ToClr(new TimeSpan(3, 4, 5), typeof(TimeOnly?)).ShouldBe(new TimeOnly(3, 4, 5));
    }

    // ---- existing behaviors pinned ----

    [Test]
    public void Null_and_DBNull_coerce_to_null()
    {
        ValueCoercion.ToClr(null, typeof(DateOnly)).ShouldBeNull();
        ValueCoercion.ToClr(DBNull.Value, typeof(int?)).ShouldBeNull();
    }

    [Test]
    public void Matching_type_passes_through_unchanged()
    {
        var date = new DateOnly(2024, 1, 2);

        ValueCoercion.ToClr(date, typeof(DateOnly)).ShouldBe(date);
        ValueCoercion.ToClr("text", typeof(string)).ShouldBe("text");
    }

    [Test]
    public void Enum_from_string_is_case_insensitive()
    {
        ValueCoercion.ToClr("saturday", typeof(DayOfWeek)).ShouldBe(DayOfWeek.Saturday);
    }

    [Test]
    public void Guid_from_string_and_bytes()
    {
        var guid = Guid.NewGuid();

        ValueCoercion.ToClr(guid.ToString(), typeof(Guid)).ShouldBe(guid);
        ValueCoercion.ToClr(guid.ToByteArray(), typeof(Guid)).ShouldBe(guid);
    }

    [Test]
    public void DateTime_from_DateTimeOffset_is_utc()
    {
        var dto = new DateTimeOffset(2024, 1, 2, 8, 0, 0, TimeSpan.FromHours(5));

        ValueCoercion.ToClr(dto, typeof(DateTime)).ShouldBe(new DateTime(2024, 1, 2, 3, 0, 0, DateTimeKind.Utc));
    }

    [Test]
    public void Convertible_numerics_fall_back_to_ChangeType()
    {
        ValueCoercion.ToClr(123L, typeof(int)).ShouldBe(123);
        ValueCoercion.ToClr((short)7, typeof(decimal)).ShouldBe(7m);
    }

    // ---- array widening (PerInstance array nullability) ----

    [Test]
    public void Value_array_widens_to_a_nullable_element_target()
    {
        // A row without NULL elements decodes as T[] even when the property is Nullable<T>[].
        ValueCoercion.ToClr(new[] { 1, 2 }, typeof(int?[])).ShouldBe(new int?[] { 1, 2 });
        ValueCoercion.ToClr(new[] { 1.5m }, typeof(decimal?[])).ShouldBe(new decimal?[] { 1.5m });
        ValueCoercion.ToClr(Array.Empty<long>(), typeof(long?[])).ShouldBeOfType<long?[]>().ShouldBeEmpty();
    }

    [Test]
    public void Nullable_element_array_passes_through_to_a_matching_target()
    {
        var value = new int?[] { 1, null };

        ValueCoercion.ToClr(value, typeof(int?[])).ShouldBeSameAs(value);
    }
}
