using System.Collections.Concurrent;
using System.Globalization;

namespace Wallaby.Providers;

/// <summary>
/// Coerces a raw value produced by the pgoutput decoder into a target CLR type, handling common
/// mismatches (enums, Guids, date/times, numerics). Storage providers layer their own conversion
/// machinery (e.g. EF Core value converters) on top of this.
/// </summary>
public static class ValueCoercion
{
    // Cache the case-insensitive enum-name → underlying-value map per enum type. Enum.Parse goes
    // through reflection + IL emit on first call; afterwards it's still a few dictionary probes,
    // but caching collapses it to a single lookup per row.
    private static readonly ConcurrentDictionary<Type, Dictionary<string, object>> EnumByName = new();

    /// <summary>Coerce <paramref name="rawValue"/> to <paramref name="targetType"/> (nullable-aware; null/DBNull → null).</summary>
    public static object? ToClr(object? rawValue, Type targetType)
    {
        if (rawValue is null || rawValue is DBNull)
        {
            return null;
        }

        var target = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (target.IsInstanceOfType(rawValue))
        {
            return rawValue;
        }

        if (target.IsEnum)
        {
            if (rawValue is string enumText)
            {
                var map = EnumByName.GetOrAdd(target, BuildEnumMap);
                if (map.TryGetValue(enumText, out var cached))
                {
                    return cached;
                }
                return Enum.Parse(target, enumText, ignoreCase: true);
            }
            return Enum.ToObject(target, rawValue);
        }

        if (target == typeof(Guid))
        {
            return rawValue switch
            {
                string guidText => Guid.Parse(guidText),
                byte[] guidBytes => new Guid(guidBytes),
                _ => rawValue,
            };
        }

        if (target == typeof(DateTimeOffset))
        {
            return rawValue switch
            {
                DateTime dateTime => dateTime.Kind == DateTimeKind.Unspecified
                    ? new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Utc))
                    : new DateTimeOffset(dateTime),
                DateOnly dateOnly => new DateTimeOffset(dateOnly.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
                string text => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                _ => rawValue,
            };
        }

        if (target == typeof(DateTime))
        {
            return rawValue switch
            {
                DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime,
                DateOnly dateOnly => dateOnly.ToDateTime(TimeOnly.MinValue),
                string text => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal),
                _ => rawValue,
            };
        }

        // Npgsql decodes date as DateOnly, time as TimeOnly, and interval as TimeSpan; none of these
        // are IConvertible, so mismatched CLR properties need explicit bridges.
        if (target == typeof(DateOnly))
        {
            return rawValue switch
            {
                DateTime dateTime => DateOnly.FromDateTime(dateTime),
                string text => DateOnly.Parse(text, CultureInfo.InvariantCulture),
                _ => rawValue,
            };
        }

        if (target == typeof(TimeOnly))
        {
            return rawValue switch
            {
                TimeSpan timeSpan => TimeOnly.FromTimeSpan(timeSpan),
                DateTime dateTime => TimeOnly.FromDateTime(dateTime),
                string text => TimeOnly.Parse(text, CultureInfo.InvariantCulture),
                _ => rawValue,
            };
        }

        if (target == typeof(TimeSpan))
        {
            return rawValue switch
            {
                TimeOnly timeOnly => timeOnly.ToTimeSpan(),
                string text => TimeSpan.Parse(text, CultureInfo.InvariantCulture),
                _ => rawValue,
            };
        }

        // Keyset cursors persist bytea values as base64 strings.
        if (target == typeof(byte[]))
        {
            return rawValue switch
            {
                string base64 => Convert.FromBase64String(base64),
                _ => rawValue,
            };
        }

        if (rawValue is IConvertible)
        {
            return Convert.ChangeType(rawValue, target, CultureInfo.InvariantCulture);
        }

        return rawValue;
    }

    private static Dictionary<string, object> BuildEnumMap(Type enumType)
    {
        // Case-insensitive lookup so we match Enum.Parse(ignoreCase: true) semantics.
        var map = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in Enum.GetNames(enumType))
        {
            map[name] = Enum.Parse(enumType, name);
        }
        return map;
    }
}
