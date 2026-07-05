using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Serializes a <see cref="RawChange"/> to/from a self-describing UTF-8 JSON form for spilling a streamed (large)
/// transaction out of memory and reading it back at commit. Unlike <c>KeysetCodec</c> (which is told each
/// element's target type on the way out), this records a per-value type tag so the exact decoded CLR value is
/// reconstructed without the EF model — the downstream materializer then coerces it exactly as for a live change.
/// Common scalar types and single-dimensional arrays of them are tagged explicitly; anything else falls back to
/// type-tagged reflection-based JSON (round-trips within the process, which is all the spill needs — it is
/// discarded on restart). The fallback requires reflection-based serialization and is unavailable in
/// trimmed/NativeAOT hosts, where such values fail the spill with a descriptive error instead.
/// <para>
/// The codec is a shared implementation detail of the built-in spill backends, which own their own framing
/// (length-prefixed bytes on disk, a <c>bytea</c> column in the database). It exposes a single canonical UTF-8
/// byte form so both backends store identical bytes with no transcoding.
/// </para>
/// </summary>
internal static partial class SpillCodec
{
    /// <summary>Serialize a change to its canonical UTF-8 JSON bytes.</summary>
    public static byte[] Encode(RawChange change) =>
        JsonSerializer.SerializeToUtf8Bytes(ToRow(change), SpillJsonContext.Default.SpillRow);

    /// <summary>Reconstruct a change from its canonical UTF-8 JSON bytes.</summary>
    public static RawChange Decode(ReadOnlySpan<byte> utf8) => FromRow(
        JsonSerializer.Deserialize(utf8, SpillJsonContext.Default.SpillRow)
            ?? throw new InvalidOperationException("Spilled change row was null."));

    private static SpillRow ToRow(RawChange change) => new(
        change.Schema,
        change.TableName,
        change.RelationId,
        (int)change.Action,
        Encode(change.NewValues),
        change.OldValues is null ? null : Encode(change.OldValues));

    private static RawChange FromRow(SpillRow row) => new()
    {
        RelationId = row.RelationId,
        Schema = row.Schema,
        TableName = row.Table,
        Action = (ChangeAction)row.Action,
        NewValues = Decode(row.New),
        OldValues = row.Old is null ? null : Decode(row.Old),
    };

    private static SpillColumn[] Encode(IReadOnlyList<RawColumn> columns)
    {
        var result = new SpillColumn[columns.Count];
        for (var i = 0; i < columns.Count; i++)
        {
            var c = columns[i];
            if (c.IsUnchangedToast)
            {
                result[i] = new SpillColumn(c.ColumnName, true, null, null);
            }
            else
            {
                var (tag, text) = EncodeValue(c.Value);
                result[i] = new SpillColumn(c.ColumnName, false, tag, text);
            }
        }
        return result;
    }

    private static RawColumn[] Decode(SpillColumn[] columns)
    {
        var result = new RawColumn[columns.Length];
        for (var i = 0; i < columns.Length; i++)
        {
            var c = columns[i];
            result[i] = c.Toast
                ? new RawColumn { ColumnName = c.Name, Value = null, IsUnchangedToast = true }
                : new RawColumn { ColumnName = c.Name, Value = DecodeValue(c.Tag!, c.Text) };
        }
        return result;
    }

    // Tag + invariant-culture text per CLR type. Arrays of tagged scalars carry "a:<element-tag>" with a JSON
    // array of element texts. "j:<assembly-qualified-name>" is the fallback for any other type, carrying
    // reflection-serialized JSON; it round-trips within the same process (the spill never outlives one).
    private static (string Tag, string? Text) EncodeValue(object? value) => value switch
    {
        null => ("0", null),
        bool b => ("b", b ? "1" : "0"),
        short s => ("i16", s.ToString(CultureInfo.InvariantCulture)),
        int i => ("i32", i.ToString(CultureInfo.InvariantCulture)),
        long l => ("i64", l.ToString(CultureInfo.InvariantCulture)),
        decimal m => ("dec", m.ToString(CultureInfo.InvariantCulture)),
        double d => ("f64", d.ToString("R", CultureInfo.InvariantCulture)),
        float f => ("f32", f.ToString("R", CultureInfo.InvariantCulture)),
        string str => ("s", str),
        char ch => ("c", ch.ToString()),
        Guid g => ("g", g.ToString("D")),
        DateTime dt => ("dt", dt.ToString("O", CultureInfo.InvariantCulture)),
        DateTimeOffset dto => ("dto", dto.ToString("O", CultureInfo.InvariantCulture)),
        DateOnly d => ("d", d.ToString("O", CultureInfo.InvariantCulture)),
        TimeOnly t => ("t", t.ToString("O", CultureInfo.InvariantCulture)),
        TimeSpan ts => ("ts", ts.ToString("c", CultureInfo.InvariantCulture)),
        byte[] bytes => ("bytes", Convert.ToBase64String(bytes)),
        string[] a => EncodeArray("s", a),
        bool[] a => EncodeArray("b", a),
        short[] a => EncodeArray("i16", a),
        int[] a => EncodeArray("i32", a),
        long[] a => EncodeArray("i64", a),
        decimal[] a => EncodeArray("dec", a),
        double[] a => EncodeArray("f64", a),
        float[] a => EncodeArray("f32", a),
        Guid[] a => EncodeArray("g", a),
        DateTime[] a => EncodeArray("dt", a),
        DateTimeOffset[] a => EncodeArray("dto", a),
        DateOnly[] a => EncodeArray("d", a),
        TimeOnly[] a => EncodeArray("t", a),
        TimeSpan[] a => EncodeArray("ts", a),
        _ => EncodeJson(value),
    };

    private static (string Tag, string Text) EncodeArray<T>(string elementTag, T?[] items)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var item in items)
            {
                if (item is null)
                {
                    writer.WriteNullValue();
                }
                else
                {
                    writer.WriteStringValue(EncodeValue(item).Text);
                }
            }
            writer.WriteEndArray();
        }
        return ("a:" + elementTag, Encoding.UTF8.GetString(buffer.WrittenSpan));
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Reflection-based serialization only runs when IsReflectionEnabledByDefault is true; trimmed hosts throw instead.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Reflection-based serialization only runs when IsReflectionEnabledByDefault is true; AOT hosts throw instead.")]
    private static (string Tag, string Text) EncodeJson(object value)
    {
        if (JsonSerializer.IsReflectionEnabledByDefault)
        {
            var type = value.GetType();
            return ("j:" + type.AssemblyQualifiedName, JsonSerializer.Serialize(value, type));
        }
        throw new NotSupportedException(
            $"Cannot spill a value of type '{value.GetType()}': reflection-based JSON serialization is disabled " +
            "(trimmed/NativeAOT host), and the type has no explicit spill encoding.");
    }

    private static object? DecodeValue(string tag, string? text) => tag switch
    {
        "0" => null,
        "b" => text == "1",
        "i16" => short.Parse(text!, CultureInfo.InvariantCulture),
        "i32" => int.Parse(text!, CultureInfo.InvariantCulture),
        "i64" => long.Parse(text!, CultureInfo.InvariantCulture),
        "dec" => decimal.Parse(text!, CultureInfo.InvariantCulture),
        "f64" => double.Parse(text!, CultureInfo.InvariantCulture),
        "f32" => float.Parse(text!, CultureInfo.InvariantCulture),
        "s" => text,
        "c" => text![0],
        "g" => Guid.ParseExact(text!, "D"),
        "dt" => DateTime.Parse(text!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
        "dto" => DateTimeOffset.Parse(text!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
        "d" => DateOnly.Parse(text!, CultureInfo.InvariantCulture),
        "t" => TimeOnly.Parse(text!, CultureInfo.InvariantCulture),
        "ts" => TimeSpan.ParseExact(text!, "c", CultureInfo.InvariantCulture),
        "bytes" => Convert.FromBase64String(text!),
        _ when tag.StartsWith("a:", StringComparison.Ordinal) => DecodeArray(tag[2..], text!),
        _ when tag.StartsWith("j:", StringComparison.Ordinal) => DecodeJson(tag, text!),
        _ => throw new InvalidOperationException($"Unknown spilled value tag '{tag}'."),
    };

    private static object DecodeArray(string elementTag, string text)
    {
        using var doc = JsonDocument.Parse(text);
        var root = doc.RootElement;
        return elementTag switch
        {
            "s" => ToArray<string?>(root, elementTag),
            "b" => ToArray<bool>(root, elementTag),
            "i16" => ToArray<short>(root, elementTag),
            "i32" => ToArray<int>(root, elementTag),
            "i64" => ToArray<long>(root, elementTag),
            "dec" => ToArray<decimal>(root, elementTag),
            "f64" => ToArray<double>(root, elementTag),
            "f32" => ToArray<float>(root, elementTag),
            "g" => ToArray<Guid>(root, elementTag),
            "dt" => ToArray<DateTime>(root, elementTag),
            "dto" => ToArray<DateTimeOffset>(root, elementTag),
            "d" => ToArray<DateOnly>(root, elementTag),
            "t" => ToArray<TimeOnly>(root, elementTag),
            "ts" => ToArray<TimeSpan>(root, elementTag),
            _ => throw new InvalidOperationException($"Unknown spilled array element tag '{elementTag}'."),
        };

        static T[] ToArray<T>(JsonElement root, string elementTag)
        {
            var result = new T[root.GetArrayLength()];
            var i = 0;
            foreach (var element in root.EnumerateArray())
            {
                result[i++] = element.ValueKind == JsonValueKind.Null
                    ? default!
                    : (T)DecodeValue(elementTag, element.GetString())!;
            }
            return result;
        }
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Reflection-based deserialization only runs when IsReflectionEnabledByDefault is true; trimmed hosts throw instead.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Reflection-based deserialization only runs when IsReflectionEnabledByDefault is true; AOT hosts throw instead.")]
    private static object? DecodeJson(string tag, string text)
    {
        if (JsonSerializer.IsReflectionEnabledByDefault)
        {
            var typeName = tag[2..];
            var type = Type.GetType(typeName)
                ?? throw new InvalidOperationException($"Cannot resolve spilled value type '{typeName}'.");
            return JsonSerializer.Deserialize(text, type);
        }
        throw new NotSupportedException(
            "Cannot read a reflection-serialized spilled value: reflection-based JSON serialization is disabled " +
            "(trimmed/NativeAOT host).");
    }

    private sealed record SpillColumn(string Name, bool Toast, string? Tag, string? Text);

    private sealed record SpillRow(string Schema, string Table, uint RelationId, int Action, SpillColumn[] New, SpillColumn[]? Old);

    [JsonSerializable(typeof(SpillRow))]
    private sealed partial class SpillJsonContext : JsonSerializerContext;
}
