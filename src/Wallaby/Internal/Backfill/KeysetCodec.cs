using System.Text.Json;
using Wallaby.Internal.Materialization;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// Serializes/deserializes keyset cursors and fan-out lookup values to/from JSON for persistence in the
/// <c>wallaby</c> state tables, coercing each element back to its target CLR type on the way out.
/// Shared by the backfill coordinator (PK cursors) and the fan-out queue (lookup value tuples).
/// </summary>
internal static class KeysetCodec
{
    /// <summary>Serialize a single value row (e.g. a PK cursor) to JSON, or null when the row is null.</summary>
    public static string? Serialize(object?[]? values)
        => values is null ? null : JsonSerializer.Serialize(values);

    /// <summary>Serialize a set of value tuples (e.g. distinct fan-out lookup keys) to JSON.</summary>
    public static string SerializeTuples(IReadOnlyList<object?[]> tuples)
        => JsonSerializer.Serialize(tuples);

    /// <summary>Deserialize a single value row, coercing each element to the matching target type.</summary>
    public static object?[]? Deserialize(string? json, IReadOnlyList<Type> targets)
    {
        if (string.IsNullOrEmpty(json))
        {
            return null;
        }

        var elements = JsonSerializer.Deserialize<JsonElement[]>(json);
        if (elements is null)
        {
            return null;
        }

        var row = new object?[elements.Length];
        for (var i = 0; i < elements.Length; i++)
        {
            row[i] = ElementToClr(elements[i], targets[i]);
        }
        return row;
    }

    /// <summary>Deserialize a set of value tuples, coercing each element to the matching target type.</summary>
    public static IReadOnlyList<object?[]> DeserializeTuples(string json, IReadOnlyList<Type> targets)
    {
        var tuples = JsonSerializer.Deserialize<JsonElement[][]>(json);
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
