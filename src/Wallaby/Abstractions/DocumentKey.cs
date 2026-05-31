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
        var hash = new HashCode();
        foreach (var value in Values)
        {
            hash.Add(value);
        }
        return hash.ToHashCode();
    }

    /// <summary>
    /// A stable string form of the key (single values render directly; composite keys are
    /// joined with <c>|</c>). Suitable as a default sink document id.
    /// </summary>
    public override string ToString()
        => Values.Count == 1
            ? Values[0]?.ToString() ?? string.Empty
            : string.Join("|", Values.Select(v => v?.ToString() ?? string.Empty));
}
