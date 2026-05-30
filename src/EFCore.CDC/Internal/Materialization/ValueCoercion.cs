using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.CDC.Internal.Materialization;

/// <summary>
/// Converts a raw value produced by the pgoutput decoder into the CLR value expected by an EF Core
/// property, applying the property's value converter when present and coercing common type mismatches
/// (enums, Guids, date/times, numerics) otherwise.
/// </summary>
internal static class ValueCoercion
{
    // Cache the case-insensitive enum-name → underlying-value map per enum type. Enum.Parse goes
    // through reflection + IL emit on first call; afterwards it's still a few dictionary probes,
    // but caching collapses it to a single lookup per row.
    private static readonly ConcurrentDictionary<Type, Dictionary<string, object>> EnumByName = new();
    public static object? ToModelValue(object? rawValue, Type modelClrType, ValueConverter? converter)
    {
        if (converter is null)
        {
            return ToClr(rawValue, modelClrType);
        }

        if (rawValue is null)
        {
            return null;
        }

        // The converter expects the provider representation; make sure the raw value matches it first.
        var providerValue = ToClr(rawValue, converter.ProviderClrType);
        return converter.ConvertFromProvider(providerValue);
    }

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
                string text => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                _ => rawValue,
            };
        }

        if (target == typeof(DateTime))
        {
            return rawValue switch
            {
                DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime,
                string text => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal),
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
