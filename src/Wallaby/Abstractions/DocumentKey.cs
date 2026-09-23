using System.Buffers;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Wallaby.Abstractions;

/// <summary>
/// The source primary key of a row, as an ordered tuple of values. Used to correlate a
/// change with the document a transform produces, and as a dictionary key, so it implements
/// structural (sequence) equality over its <see cref="Values"/>.
/// </summary>
public sealed class DocumentKey : IEquatable<DocumentKey>
{
    private const char Separator = '|';

    // Fits every fixed-width value (numbers, Guids, ISO dates) and byte arrays up to 32 bytes.
    private const int FormatBufferSize = 64;
    private static readonly SearchValues<char> Reserved = SearchValues.Create("%|");

    /// <summary>The key values, in key ordinal order.</summary>
    public IReadOnlyList<object?> Values { get; }

    /// <summary>Creates a key from the given ordered values.</summary>
    public DocumentKey(IReadOnlyList<object?> values)
        => Values = values ?? throw new ArgumentNullException(nameof(values));

    /// <summary>Creates a single-column key.</summary>
    public DocumentKey(object? value) : this(new[] { value }) { }

    /// <inheritdoc />
    public bool Equals(DocumentKey? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        if (Values.Count != other.Values.Count) return false;
        for (var i = 0; i < Values.Count; i++)
        {
            if (!ValueEquals(Values[i], other.Values[i])) return false;
        }
        return true;
    }

    // Byte arrays (bytea keys) compare by content; every other key value by its own Equals.
    private static bool ValueEquals(object? left, object? right)
        => left is byte[] a && right is byte[] b ? a.AsSpan().SequenceEqual(b) : Equals(left, right);

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as DocumentKey);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        // Indexed to avoid the interface enumerator allocation; this runs once per change routed.
        var hash = new HashCode();
        for (var i = 0; i < Values.Count; i++)
        {
            if (Values[i] is byte[] bytes)
            {
                hash.AddBytes(bytes);
            }
            else
            {
                hash.Add(Values[i]);
            }
        }
        return hash.ToHashCode();
    }

    /// <summary>
    /// The canonical document id: each value rendered culture-invariantly (date and time values in ISO 8601
    /// round-trip form, byte arrays as lowercase hex, null as empty), with <c>%</c> and <c>|</c> inside a value
    /// escaped as <c>%25</c> and <c>%7C</c>, and the values joined with <c>|</c>. Distinct keys of one table
    /// always produce distinct ids, and <see cref="SplitId"/> recovers the rendered values.
    /// </summary>
    public override string ToString()
    {
        if (Values.Count == 1)
        {
            return FormatId(Values[0]);
        }

        var builder = new DefaultInterpolatedStringHandler(0, 0, null, stackalloc char[FormatBufferSize]);
        Span<char> buffer = stackalloc char[FormatBufferSize];
        for (var i = 0; i < Values.Count; i++)
        {
            if (i > 0)
            {
                builder.AppendFormatted(Separator);
            }
            AppendEscaped(ref builder, Format(Values[i], buffer));
        }
        return builder.ToStringAndClear();
    }

    /// <summary>
    /// The document id of a single-value key holding <paramref name="value"/>: the same string as
    /// <c>new DocumentKey(value).ToString()</c>, without allocating the key.
    /// </summary>
    /// <param name="value">The key value.</param>
    internal static string FormatId(object? value)
    {
        if (value is string s)
        {
            return s.AsSpan().ContainsAny(Reserved) ? Escape(s) : s;
        }

        Span<char> buffer = stackalloc char[FormatBufferSize];
        var formatted = Format(value, buffer);
        return formatted.ContainsAny(Reserved) ? Escape(formatted) : new string(formatted);
    }

    /// <summary>
    /// Splits a document id produced by <see cref="ToString"/> back into its rendered values, in key order.
    /// </summary>
    /// <param name="documentId">A canonical document id.</param>
    /// <exception cref="FormatException">The id contains a <c>%</c> that is not a <c>%25</c> or <c>%7C</c> escape.</exception>
    public static IReadOnlyList<string> SplitId(string documentId)
    {
        ArgumentNullException.ThrowIfNull(documentId);

        var parts = new List<string>();
        foreach (var range in documentId.AsSpan().Split(Separator))
        {
            parts.Add(Unescape(documentId.AsSpan(range), documentId));
        }
        return parts;
    }

    private static string Unescape(ReadOnlySpan<char> part, string documentId)
    {
        if (!part.Contains('%'))
        {
            // The whole id as one value needs no copy.
            return part.Length == documentId.Length ? documentId : new string(part);
        }

        var builder = new DefaultInterpolatedStringHandler(0, 0, null, stackalloc char[FormatBufferSize]);
        int index;
        while ((index = part.IndexOf('%')) >= 0)
        {
            var escape = part[index..];
            builder.AppendFormatted(part[..index]);
            builder.AppendFormatted(
                escape.StartsWith("%25") ? '%'
                : escape.StartsWith("%7C") ? Separator
                : throw new FormatException(
                    $"Document id '{documentId}' has an invalid escape; only %25 and %7C are produced."));
            part = escape[3..];
        }
        builder.AppendFormatted(part);
        return builder.ToStringAndClear();
    }

    private static string Escape(ReadOnlySpan<char> value)
    {
        var builder = new DefaultInterpolatedStringHandler(0, 0, null, stackalloc char[FormatBufferSize]);
        AppendEscaped(ref builder, value);
        return builder.ToStringAndClear();
    }

    private static void AppendEscaped(ref DefaultInterpolatedStringHandler builder, ReadOnlySpan<char> value)
    {
        int index;
        while ((index = value.IndexOfAny(Reserved)) >= 0)
        {
            builder.AppendFormatted(value[..index]);
            builder.AppendFormatted(value[index] == '%' ? "%25" : "%7C");
            value = value[(index + 1)..];
        }
        builder.AppendFormatted(value);
    }

    /// <summary>
    /// Formats a key value culture-invariantly: into <paramref name="buffer"/> when it fits, otherwise as a
    /// string. Date and time values use the ISO 8601 round-trip form.
    /// </summary>
    private static ReadOnlySpan<char> Format(object? value, Span<char> buffer)
    {
        switch (value)
        {
            case null:
                return [];
            case string s:
                return s;
            case byte[] bytes:
                return Convert.TryToHexStringLower(bytes, buffer, out var hexLength)
                    ? buffer[..hexLength]
                    : Convert.ToHexStringLower(bytes);
            case ISpanFormattable formattable:
                var format = value is DateTime or DateTimeOffset or DateOnly or TimeOnly ? "O" : null;
                return formattable.TryFormat(buffer, out var written, format, CultureInfo.InvariantCulture)
                    ? buffer[..written]
                    : formattable.ToString(format, CultureInfo.InvariantCulture);
            case IFormattable formattable:
                return formattable.ToString(null, CultureInfo.InvariantCulture);
            default:
                return value.ToString();
        }
    }
}
