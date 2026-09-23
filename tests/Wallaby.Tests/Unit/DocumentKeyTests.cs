using System.Globalization;
using Wallaby.Abstractions;

namespace Wallaby.Tests.Unit;

/// <summary>
/// Canonical document ids: culture-invariant, lossless for date and time values, escaped so composite ids
/// stay unambiguous, and splittable back into their rendered values.
/// </summary>
public class DocumentKeyTests
{
    [Test]
    public void Values_render_culture_invariantly()
    {
        var previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("de-DE");
        try
        {
            new DocumentKey(1.5).ToString().ShouldBe("1.5");
            new DocumentKey(1234567L).ToString().ShouldBe("1234567");
            new DocumentKey(new DateTime(2026, 9, 17, 10, 30, 0, DateTimeKind.Utc)).ToString()
                .ShouldBe("2026-09-17T10:30:00.0000000Z");
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Test]
    public void Date_and_time_values_render_in_iso_round_trip_form()
    {
        new DocumentKey(new DateTimeOffset(2026, 9, 17, 10, 30, 15, 123, TimeSpan.FromHours(2))).ToString()
            .ShouldBe("2026-09-17T10:30:15.1230000+02:00");
        new DocumentKey(new DateOnly(2026, 9, 17)).ToString().ShouldBe("2026-09-17");
        new DocumentKey(new TimeOnly(10, 30, 15, 123)).ToString().ShouldBe("10:30:15.1230000");
    }

    [Test]
    public void Sub_second_date_and_time_values_stay_distinct()
    {
        var at = new DateTime(2026, 9, 17, 10, 30, 15, DateTimeKind.Utc);
        new DocumentKey(["device-1", at]).ToString()
            .ShouldNotBe(new DocumentKey(["device-1", at.AddMilliseconds(1)]).ToString());
        new DocumentKey(new TimeOnly(10, 30, 15)).ToString()
            .ShouldNotBe(new DocumentKey(new TimeOnly(10, 30, 16)).ToString());
    }

    [Test]
    public void Byte_arrays_render_as_lowercase_hex()
    {
        new DocumentKey(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }).ToString().ShouldBe("deadbeef");
    }

    [Test]
    public void Composite_keys_join_with_a_pipe_and_null_renders_empty()
    {
        new DocumentKey([42, "tenant-a", null]).ToString().ShouldBe("42|tenant-a|");
    }

    [Test]
    public void Guids_and_strings_render_directly()
    {
        var id = Guid.NewGuid();
        new DocumentKey(id).ToString().ShouldBe(id.ToString());
        new DocumentKey("sku-1").ToString().ShouldBe("sku-1");
        new DocumentKey([7, id]).ToString().ShouldBe($"7|{id}");
    }

    [Test]
    public void Separator_and_escape_characters_inside_values_are_escaped()
    {
        new DocumentKey("a|b").ToString().ShouldBe("a%7Cb");
        new DocumentKey("100%").ToString().ShouldBe("100%25");
        new DocumentKey(["a|b", "c"]).ToString()
            .ShouldNotBe(new DocumentKey(["a", "b|c"]).ToString());
    }

    [Test]
    [Arguments("plain")]
    [Arguments("a|b")]
    [Arguments("100%")]
    [Arguments("%7C")]
    [Arguments("")]
    public void Split_id_recovers_a_single_value(string value)
    {
        DocumentKey.SplitId(new DocumentKey(value).ToString()).ShouldBe([value]);
    }

    [Test]
    public void Split_id_recovers_composite_values()
    {
        DocumentKey.SplitId(new DocumentKey(["a|b", "%25", 42, null]).ToString())
            .ShouldBe(["a|b", "%25", "42", ""]);
    }

    [Test]
    [Arguments("a%")]
    [Arguments("a%2")]
    [Arguments("a%41")]
    public void Split_id_rejects_escapes_it_never_produces(string documentId)
    {
        Should.Throw<FormatException>(() => DocumentKey.SplitId(documentId));
    }
}
