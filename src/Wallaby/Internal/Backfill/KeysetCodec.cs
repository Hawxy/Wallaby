using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Wallaby.Providers;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// Serializes/deserializes keyset cursors and fan-out lookup values to/from JSON for persistence in the
/// <c>wallaby</c> state tables, coercing each element back to its target CLR type on the way out.
/// A cursor is persisted as a versioned envelope carrying the primary-key column names it was built
/// against, so a cursor that no longer matches the table's key shape is rejected (the caller restarts
/// the backfill) instead of being silently misread.
/// </summary>
internal static class KeysetCodec
{
    private const int CursorVersion = 1;

    /// <summary>Serialize a PK cursor and the column names it indexes, or null when the cursor is null.</summary>
    public static string? SerializeCursor(object?[]? values, IReadOnlyList<string> pkColumns)
    {
        if (values is null)
        {
            return null;
        }

        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("v"u8, CursorVersion);
            writer.WriteStartArray("pk"u8);
            foreach (var column in pkColumns)
            {
                writer.WriteStringValue(column);
            }
            writer.WriteEndArray();
            writer.WriteStartArray("cur"u8);
            foreach (var value in values)
            {
                WriteValue(writer, value);
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>
    /// Deserialize a PK cursor, coercing each element to the matching target type. Returns false when the
    /// persisted JSON is not a current-version envelope for exactly <paramref name="pkColumns"/> (ordinal,
    /// order-sensitive) — the caller restarts from scratch. Null/empty input is a valid "no cursor yet".
    /// </summary>
    public static bool TryDeserializeCursor(
        string? json, IReadOnlyList<string> pkColumns, IReadOnlyList<Type> targets, out object?[]? cursor)
    {
        cursor = null;
        if (string.IsNullOrEmpty(json))
        {
            return true;
        }

        CursorEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize(json, KeysetJsonContext.Default.CursorEnvelope);
        }
        catch (JsonException)
        {
            return false;
        }

        if (envelope is null || envelope.V != CursorVersion || envelope.Pk is null || envelope.Cur is null ||
            envelope.Pk.Length != pkColumns.Count || envelope.Cur.Length != targets.Count)
        {
            return false;
        }

        for (var i = 0; i < pkColumns.Count; i++)
        {
            if (!string.Equals(envelope.Pk[i], pkColumns[i], StringComparison.Ordinal))
            {
                return false;
            }
        }

        var row = new object?[envelope.Cur.Length];
        for (var i = 0; i < row.Length; i++)
        {
            row[i] = ElementToClr(envelope.Cur[i], targets[i]);
        }
        cursor = row;
        return true;
    }

    /// <summary>Serialize a set of value tuples (e.g. distinct fan-out lookup keys) to JSON.</summary>
    public static string SerializeTuples(IReadOnlyList<object?[]> tuples)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var tuple in tuples)
            {
                writer.WriteStartArray();
                foreach (var value in tuple)
                {
                    WriteValue(writer, value);
                }
                writer.WriteEndArray();
            }
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>Deserialize a set of value tuples, coercing each element to the matching target type.</summary>
    public static IReadOnlyList<object?[]> DeserializeTuples(string json, IReadOnlyList<Type> targets)
    {
        var tuples = JsonSerializer.Deserialize(json, KeysetJsonContext.Default.JsonElementArrayArray);
        if (tuples is null)
        {
            return [];
        }

        var result = new List<object?[]>(tuples.Length);
        foreach (var tuple in tuples)
        {
            var row = new object?[tuple.Length];
            for (var i = 0; i < tuple.Length; i++)
            {
                row[i] = ElementToClr(tuple[i], targets[i]);
            }
            result.Add(row);
        }
        return result;
    }

    // Values are written in the shapes ElementToClr reads back: numbers/bools/nulls natively, everything
    // else as an invariant string that ValueCoercion.ToClr parses against the target type (Guid as "D",
    // date/times as ISO 8601 via the Utf8JsonWriter overloads).
    private static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null or DBNull: writer.WriteNullValue(); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case byte n: writer.WriteNumberValue(n); break;
            case sbyte n: writer.WriteNumberValue(n); break;
            case short n: writer.WriteNumberValue(n); break;
            case ushort n: writer.WriteNumberValue(n); break;
            case int n: writer.WriteNumberValue(n); break;
            case uint n: writer.WriteNumberValue(n); break;
            case long n: writer.WriteNumberValue(n); break;
            case ulong n: writer.WriteNumberValue(n); break;
            case decimal n: writer.WriteNumberValue(n); break;
            case double n: writer.WriteNumberValue(n); break;
            case float n: writer.WriteNumberValue(n); break;
            case string s: writer.WriteStringValue(s); break;
            case Guid g: writer.WriteStringValue(g); break;
            case DateTime dt: writer.WriteStringValue(dt); break;
            case DateTimeOffset dto: writer.WriteStringValue(dto); break;
            default: writer.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture)); break;
        }
    }

    private static object? ElementToClr(JsonElement element, Type target)
    {
        var underlying = Nullable.GetUnderlyingType(target) ?? target;
        return element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True or JsonValueKind.False => element.GetBoolean(),
            JsonValueKind.String => ValueCoercion.ToClr(element.GetString(), target),
            JsonValueKind.Number when underlying == typeof(long) => element.GetInt64(),
            JsonValueKind.Number when underlying == typeof(int) || underlying == typeof(short) => element.GetInt32(),
            JsonValueKind.Number when underlying == typeof(decimal) => element.GetDecimal(),
            JsonValueKind.Number when underlying == typeof(double) || underlying == typeof(float) => element.GetDouble(),
            JsonValueKind.Number => element.GetInt64(),
            _ => ValueCoercion.ToClr(element.GetRawText(), target),
        };
    }
}

/// <summary>The persisted shape of a keyset cursor: format version, PK column names, cursor values.</summary>
internal sealed class CursorEnvelope
{
    [JsonPropertyName("v")]
    public int V { get; init; }

    [JsonPropertyName("pk")]
    public string[]? Pk { get; init; }

    [JsonPropertyName("cur")]
    public JsonElement[]? Cur { get; init; }
}

[JsonSerializable(typeof(CursorEnvelope))]
[JsonSerializable(typeof(JsonElement[][]))]
internal sealed partial class KeysetJsonContext : JsonSerializerContext;
