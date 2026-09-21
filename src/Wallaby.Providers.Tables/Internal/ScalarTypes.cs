using System.Collections;
using System.Net;
using System.Net.NetworkInformation;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>
/// The property types the provider maps to a single column: what the pgoutput decoder and backfill
/// reader produce, or what <see cref="ValueCoercion"/> can bridge to. Nested objects and collections
/// (JSON-shaped data) are outside this set and fail at registration.
/// </summary>
internal static class ScalarTypes
{
    private static readonly HashSet<Type> Scalars =
    [
        typeof(bool), typeof(byte), typeof(sbyte), typeof(short), typeof(ushort), typeof(int), typeof(uint),
        typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal), typeof(char),
        typeof(string), typeof(Guid), typeof(DateTime), typeof(DateTimeOffset), typeof(DateOnly),
        typeof(TimeOnly), typeof(TimeSpan), typeof(byte[]), typeof(IPAddress), typeof(PhysicalAddress),
        typeof(BitArray),
    ];

    // The types a keyset cursor persists and binds back as a query parameter (KeysetCodec.WriteValue).
    // Enums are excluded: a resumed cursor would rebind a text-backed enum as a CLR enum, which Npgsql
    // cannot write.
    private static readonly HashSet<Type> KeyScalars =
    [
        typeof(bool), typeof(byte), typeof(sbyte), typeof(short), typeof(ushort), typeof(int), typeof(uint),
        typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal), typeof(char),
        typeof(string), typeof(Guid), typeof(DateTime), typeof(DateTimeOffset), typeof(DateOnly),
        typeof(TimeOnly), typeof(TimeSpan), typeof(byte[]),
    ];

    public static bool IsSupported(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        if (Scalars.Contains(underlying) || underlying.IsEnum)
        {
            return true;
        }
        if (underlying.IsArray && underlying.GetArrayRank() == 1 && underlying != typeof(byte[]))
        {
            var element = underlying.GetElementType()!;
            var elementUnderlying = Nullable.GetUnderlyingType(element) ?? element;
            return Scalars.Contains(elementUnderlying) || elementUnderlying.IsEnum;
        }
        return false;
    }

    public static bool IsSupportedKey(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return KeyScalars.Contains(underlying);
    }

    /// <summary>
    /// <c>default(T)</c> for a non-nullable value type, for constructor parameters whose column is absent
    /// from the change. Explicit arms keep this reflection-free for NativeAOT.
    /// </summary>
    public static object? DefaultValue(Type type)
    {
        if (!type.IsValueType || Nullable.GetUnderlyingType(type) is not null)
        {
            return null;
        }
        if (type.IsEnum)
        {
            return Enum.ToObject(type, 0);
        }
        return type switch
        {
            _ when type == typeof(bool) => false,
            _ when type == typeof(byte) => (byte)0,
            _ when type == typeof(sbyte) => (sbyte)0,
            _ when type == typeof(short) => (short)0,
            _ when type == typeof(ushort) => (ushort)0,
            _ when type == typeof(int) => 0,
            _ when type == typeof(uint) => 0u,
            _ when type == typeof(long) => 0L,
            _ when type == typeof(ulong) => 0ul,
            _ when type == typeof(float) => 0f,
            _ when type == typeof(double) => 0d,
            _ when type == typeof(decimal) => 0m,
            _ when type == typeof(char) => '\0',
            _ when type == typeof(Guid) => Guid.Empty,
            _ when type == typeof(DateTime) => default(DateTime),
            _ when type == typeof(DateTimeOffset) => default(DateTimeOffset),
            _ when type == typeof(DateOnly) => default(DateOnly),
            _ when type == typeof(TimeOnly) => default(TimeOnly),
            _ when type == typeof(TimeSpan) => TimeSpan.Zero,
            _ => throw new InvalidOperationException($"No default value is known for '{type}'."),
        };
    }
}
