using System.Buffers;
using System.Globalization;
using System.Text;

namespace Wallaby.Abstractions;

/// <summary>
/// The source primary key of a row, as an ordered tuple of values. Used to correlate a
/// change with the document a transform produces, and as a dictionary key, so it implements
/// structural (sequence) equality over its <see cref="Values"/>.
/// </summary>
public sealed class DocumentKey : IEquatable<DocumentKey>
{
    private const char Separator = '|';
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
            if (!Equals(Values[i], other.Values[i])) return false;
        }
        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as DocumentKey);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        // Indexed to avoid the interface enumerator allocation; this runs once per change routed.
        var hash = new HashCode();
        for (var i = 0; i < Values.Count; i++)
        {
            hash.Add(Values[i]);
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
            var single = Format(Values[0]);
            return single.AsSpan().ContainsAny(Reserved)
                ? AppendEscaped(new StringBuilder(single.Length + 4), single).ToString()
                : single;
        }

        var sb = new StringBuilder();
        for (var i = 0; i < Values.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(Separator);
            }
            AppendEscaped(sb, Format(Values[i]));
        }
        return sb.ToString();
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
        var sb = new StringBuilder();
        for (var i = 0; i < documentId.Length; i++)
        {
            var ch = documentId[i];
            if (ch == Separator)
            {
                parts.Add(sb.ToString());
                sb.Clear();
            }
            else if (ch == '%')
            {
                var escape = i + 2 < documentId.Length ? documentId.AsSpan(i + 1, 2) : [];
                sb.Append(escape switch
                {
                    "25" => '%',
                    "7C" => Separator,
                    _ => throw new FormatException(
                        $"Document id '{documentId}' has an invalid escape at position {i}; only %25 and %7C are produced."),
                });
                i += 2;
            }
            else
            {
                sb.Append(ch);
            }
        }
        parts.Add(sb.ToString());
        return parts;
    }

    private static StringBuilder AppendEscaped(StringBuilder sb, string value)
    {
        var rest = value.AsSpan();
        int index;
        while ((index = rest.IndexOfAny(Reserved)) >= 0)
        {
            sb.Append(rest[..index]).Append(rest[index] == '%' ? "%25" : "%7C");
            rest = rest[(index + 1)..];
        }
        return sb.Append(rest);
    }

    private static string Format(object? value) => value switch
    {
        null => string.Empty,
        string s => s,
        byte[] bytes => Convert.ToHexStringLower(bytes),
        DateTime dateTime => dateTime.ToString("O", CultureInfo.InvariantCulture),
        DateTimeOffset dateTimeOffset => dateTimeOffset.ToString("O", CultureInfo.InvariantCulture),
        DateOnly date => date.ToString("O", CultureInfo.InvariantCulture),
        TimeOnly time => time.ToString("O", CultureInfo.InvariantCulture),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? string.Empty,
    };
}
