using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net;
using System.Net.NetworkInformation;
using System.Text.Json;
using System.Text.Json.Serialization;
using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Serializes a <see cref="RawChange"/> to/from a self-describing UTF-8 JSON form for spilling a streamed (large)
/// transaction out of memory and reading it back at commit. Unlike <c>KeysetCodec</c> (which is told each
/// element's target type on the way out), this records a per-value type tag so the exact decoded CLR value is
/// reconstructed without the EF model; the downstream materializer then coerces it as for a live change.
/// Common scalar types and single-dimensional arrays of them are tagged explicitly; anything else falls back to
/// type-tagged reflection-based JSON, which round-trips within the process (the spill is discarded on restart).
/// The fallback is unavailable in trimmed/NativeAOT hosts, where such values fail the spill with a descriptive
/// error instead.
/// <para>
/// Shared by the built-in spill backends, which own their own framing (length-prefixed bytes on disk, a
/// <c>bytea</c> column in the database); both store the same canonical UTF-8 bytes.
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
                result[i] = new SpillColumn(c.ColumnName, true, null, null, null);
            }
            else if (c.Value is Array array && ArrayTag(array) is { } arrayTag)
            {
                result[i] = new SpillColumn(c.ColumnName, false, arrayTag, null, EncodeArray(array));
            }
            else
            {
                var (tag, text) = EncodeValue(c.Value);
                result[i] = new SpillColumn(c.ColumnName, false, tag, text, null);
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
                : new RawColumn
                {
                    ColumnName = c.Name,
                    Value = c.Items is { } items ? DecodeTaggedArray(c.Tag!, items) : DecodeValue(c.Tag!, c.Text),
                };
        }
        return result;
    }

    // Tag + invariant-culture text per scalar CLR type. "j:<assembly-qualified-name>" is the fallback for any
    // other type, carrying reflection-serialized JSON; it round-trips within the same process (the spill never
    // outlives one).
    private static (string Tag, string? Text) EncodeValue(object? value) => value switch
    {
        null => ("0", null),
        bool b => ("b", b ? "1" : "0"),
        byte u8 => ("u8", u8.ToString(CultureInfo.InvariantCulture)),
        short s => ("i16", s.ToString(CultureInfo.InvariantCulture)),
        int i => ("i32", i.ToString(CultureInfo.InvariantCulture)),
        long l => ("i64", l.ToString(CultureInfo.InvariantCulture)),
        uint u => ("u32", u.ToString(CultureInfo.InvariantCulture)),
        ulong ul => ("u64", ul.ToString(CultureInfo.InvariantCulture)),
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
        IPAddress ip => ("ip", ip.ToString()),
        PhysicalAddress mac => ("mac", mac.ToString()),
        BitArray bits => ("bits", EncodeBits(bits)),
        // CLR array variance lets an sbyte[] match this pattern, so the arm checks the exact runtime type.
        byte[] bytes when bytes.GetType() == typeof(byte[]) => ("bytes", Convert.ToBase64String(bytes)),
        _ => EncodeJson(value),
    };

    // Arrays of tagged scalars carry "a:<element-tag>" with one text per element; Nullable<T>[] arrays (how a
    // NULL element decodes under PerInstance array nullability) carry "an:<element-tag>" so null elements
    // survive the round-trip. Null for an array with no tagged element type (byte[] is a scalar, base64).
    private static string? ArrayTag(Array value) => value switch
    {
        // CLR array variance lets an unsigned (or enum) array match its signed pattern, so the
        // integral array arms dispatch on the exact runtime type; anything else falls to the fallback.
        string[] => "a:s",
        bool[] => "a:b",
        short[] when value.GetType() == typeof(short[]) => "a:i16",
        int[] when value.GetType() == typeof(int[]) => "a:i32",
        long[] when value.GetType() == typeof(long[]) => "a:i64",
        uint[] when value.GetType() == typeof(uint[]) => "a:u32",
        decimal[] => "a:dec",
        double[] => "a:f64",
        float[] => "a:f32",
        Guid[] => "a:g",
        DateTime[] => "a:dt",
        DateTimeOffset[] => "a:dto",
        DateOnly[] => "a:d",
        TimeOnly[] => "a:t",
        TimeSpan[] => "a:ts",
        IPAddress?[] => "a:ip",
        bool?[] => "an:b",
        short?[] => "an:i16",
        int?[] => "an:i32",
        long?[] => "an:i64",
        uint?[] => "an:u32",
        decimal?[] => "an:dec",
        double?[] => "an:f64",
        float?[] => "an:f32",
        Guid?[] => "an:g",
        DateTime?[] => "an:dt",
        DateTimeOffset?[] => "an:dto",
        DateOnly?[] => "an:d",
        TimeOnly?[] => "an:t",
        TimeSpan?[] => "an:ts",
        _ => null,
    };

    // Non-generic on purpose: boxing a null Nullable<T> yields a null reference, so one enumeration
    // handles T[], T?[], and reference-element arrays alike.
    private static string?[] EncodeArray(Array items)
    {
        var result = new string?[items.Length];
        var i = 0;
        foreach (var item in items)
        {
            result[i++] = item is null ? null : EncodeValue(item).Text;
        }
        return result;
    }

    private static string EncodeBits(BitArray bits) => string.Create(bits.Length, bits, static (span, b) =>
    {
        for (var i = 0; i < span.Length; i++)
        {
            span[i] = b[i] ? '1' : '0';
        }
    });

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
        "u8" => byte.Parse(text!, CultureInfo.InvariantCulture),
        "i16" => short.Parse(text!, CultureInfo.InvariantCulture),
        "i32" => int.Parse(text!, CultureInfo.InvariantCulture),
        "i64" => long.Parse(text!, CultureInfo.InvariantCulture),
        "u32" => uint.Parse(text!, CultureInfo.InvariantCulture),
        "u64" => ulong.Parse(text!, CultureInfo.InvariantCulture),
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
        "ip" => IPAddress.Parse(text!),
        "mac" => PhysicalAddress.Parse(text!),
        "bits" => DecodeBits(text!),
        "bytes" => Convert.FromBase64String(text!),

        _ when tag.StartsWith("j:", StringComparison.Ordinal) => DecodeJson(tag, text!),
        _ => throw new InvalidOperationException($"Unknown spilled value tag '{tag}'."),
    };

    private static object DecodeTaggedArray(string tag, string?[] items)
        => tag.StartsWith("an:", StringComparison.Ordinal)
            ? DecodeNullableArray(tag[3..], items)
            : DecodeArray(tag[2..], items);

    private static object DecodeArray(string elementTag, string?[] items)
    {
        return elementTag switch
        {
            "s" => ToArray<string?>(items, elementTag),
            "b" => ToArray<bool>(items, elementTag),
            "i16" => ToArray<short>(items, elementTag),
            "i32" => ToArray<int>(items, elementTag),
            "i64" => ToArray<long>(items, elementTag),
            "u32" => ToArray<uint>(items, elementTag),
            "dec" => ToArray<decimal>(items, elementTag),
            "f64" => ToArray<double>(items, elementTag),
            "f32" => ToArray<float>(items, elementTag),
            "g" => ToArray<Guid>(items, elementTag),
            "dt" => ToArray<DateTime>(items, elementTag),
            "dto" => ToArray<DateTimeOffset>(items, elementTag),
            "d" => ToArray<DateOnly>(items, elementTag),
            "t" => ToArray<TimeOnly>(items, elementTag),
            "ts" => ToArray<TimeSpan>(items, elementTag),
            "ip" => ToArray<IPAddress?>(items, elementTag),
            _ => throw new InvalidOperationException($"Unknown spilled array element tag '{elementTag}'."),
        };

        static T[] ToArray<T>(string?[] items, string elementTag)
        {
            var result = new T[items.Length];
            for (var i = 0; i < items.Length; i++)
            {
                result[i] = items[i] is null ? default! : (T)DecodeValue(elementTag, items[i])!;
            }
            return result;
        }
    }

    private static object DecodeNullableArray(string elementTag, string?[] items)
    {
        return elementTag switch
        {
            "b" => ToNullableArray<bool>(items, elementTag),
            "i16" => ToNullableArray<short>(items, elementTag),
            "i32" => ToNullableArray<int>(items, elementTag),
            "i64" => ToNullableArray<long>(items, elementTag),
            "u32" => ToNullableArray<uint>(items, elementTag),
            "dec" => ToNullableArray<decimal>(items, elementTag),
            "f64" => ToNullableArray<double>(items, elementTag),
            "f32" => ToNullableArray<float>(items, elementTag),
            "g" => ToNullableArray<Guid>(items, elementTag),
            "dt" => ToNullableArray<DateTime>(items, elementTag),
            "dto" => ToNullableArray<DateTimeOffset>(items, elementTag),
            "d" => ToNullableArray<DateOnly>(items, elementTag),
            "t" => ToNullableArray<TimeOnly>(items, elementTag),
            "ts" => ToNullableArray<TimeSpan>(items, elementTag),
            _ => throw new InvalidOperationException($"Unknown spilled nullable-array element tag '{elementTag}'."),
        };

        static T?[] ToNullableArray<T>(string?[] items, string elementTag) where T : struct
        {
            var result = new T?[items.Length];
            for (var i = 0; i < items.Length; i++)
            {
                result[i] = items[i] is null ? null : (T)DecodeValue(elementTag, items[i])!;
            }
            return result;
        }
    }

    private static BitArray DecodeBits(string text)
    {
        var bits = new BitArray(text.Length);
        for (var i = 0; i < text.Length; i++)
        {
            bits[i] = text[i] == '1';
        }
        return bits;
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

    private sealed record SpillColumn(string Name, bool Toast, string? Tag, string? Text, string?[]? Items);

    private sealed record SpillRow(string Schema, string Table, uint RelationId, int Action, SpillColumn[] New, SpillColumn[]? Old);

    [JsonSerializable(typeof(SpillRow))]
    [JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    private sealed partial class SpillJsonContext : JsonSerializerContext;
}
