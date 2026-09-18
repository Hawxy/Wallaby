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
    /// A stable, culture-invariant string form of the key: single values render directly, composite
    /// keys are joined with <c>|</c>, byte arrays render as lowercase hex, and null renders empty.
    /// Suitable as a default sink document id.
    /// </summary>
    public override string ToString()
    {
        if (Values.Count == 1)
        {
            return Format(Values[0]);
        }

        var sb = new StringBuilder();
        for (var i = 0; i < Values.Count; i++)
        {
            if (i > 0)
            {
                sb.Append('|');
            }
            sb.Append(Format(Values[i]));
        }
        return sb.ToString();
    }

    /// <summary>Render one key value the way <see cref="ToString"/> does.</summary>
    internal static string Format(object? value) => value switch
    {
        null => string.Empty,
        string s => s,
        byte[] bytes => Convert.ToHexStringLower(bytes),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? string.Empty,
    };
}
