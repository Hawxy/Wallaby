using System.Globalization;
using Wallaby.Abstractions;

namespace Wallaby.Tests.Unit;

/// <summary>Document ids derived from keys render the same under every culture.</summary>
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
                .ShouldBe("09/17/2026 10:30:00");
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
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
    }
}
