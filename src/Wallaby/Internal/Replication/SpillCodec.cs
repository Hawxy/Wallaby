using System.Globalization;
using System.Text.Json;
using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Serializes a <see cref="RawChange"/> to/from a self-describing UTF-8 JSON form for spilling a streamed (large)
/// transaction out of memory and reading it back at commit. Unlike <c>KeysetCodec</c> (which is told each
/// element's target type on the way out), this records a per-value type tag so the exact decoded CLR value is
/// reconstructed without the EF model — the downstream materializer then coerces it exactly as for a live change.
/// Common scalar types are tagged explicitly; arrays and anything else fall back to type-tagged JSON
/// (round-trips within the process, which is all the spill needs — it is discarded on restart).
/// <para>
/// The codec is a shared implementation detail of the built-in spill backends, which own their own framing
/// (length-prefixed bytes on disk, a <c>bytea</c> column in the database). It exposes a single canonical UTF-8
/// byte form so both backends store identical bytes with no transcoding.
/// </para>
/// </summary>
internal static class SpillCodec
{
    /// <summary>Serialize a change to its canonical UTF-8 JSON bytes.</summary>
    public static byte[] Encode(RawChange change) => JsonSerializer.SerializeToUtf8Bytes(ToRow(change));

    /// <summary>Reconstruct a change from its canonical UTF-8 JSON bytes.</summary>
    public static RawChange Decode(ReadOnlySpan<byte> utf8) => FromRow(
        JsonSerializer.Deserialize<SpillRow>(utf8) ?? throw new InvalidOperationException("Spilled change row was null."));

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

    // Tag + invariant-culture text per CLR type. "j:<assembly-qualified-name>" is the fallback for arrays and any
    // other type, carrying STJ JSON; it round-trips within the same process (the spill never outlives one).
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
        _ => EncodeJson(value),
    };

    private static (string Tag, string Text) EncodeJson(object value)
    {
        var type = value.GetType();
        return ("j:" + type.AssemblyQualifiedName, JsonSerializer.Serialize(value, type));
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
        _ when tag.StartsWith("j:", StringComparison.Ordinal) => DecodeJson(tag, text!),
        _ => throw new InvalidOperationException($"Unknown spilled value tag '{tag}'."),
    };

    private static object? DecodeJson(string tag, string text)
    {
        var typeName = tag[2..];
        var type = Type.GetType(typeName)
            ?? throw new InvalidOperationException($"Cannot resolve spilled value type '{typeName}'.");
        return JsonSerializer.Deserialize(text, type);
    }

    private sealed record SpillColumn(string Name, bool Toast, string? Tag, string? Text);

    private sealed record SpillRow(string Schema, string Table, uint RelationId, int Action, SpillColumn[] New, SpillColumn[]? Old);
}
