namespace Wallaby.Abstractions;

/// <summary>
/// The document a transform emits for a source key: a field bag keyed by destination field name.
/// It derives from <see cref="Dictionary{TKey,TValue}"/>, so it supports the usual initializer syntax
/// (<c>new CdcDocument { ["name"] = product.Name }</c>) and is consumed by sinks as an
/// <see cref="IReadOnlyDictionary{TKey,TValue}"/> with no extra allocation.
/// </summary>
public sealed class CdcDocument : Dictionary<string, object?>
{
    /// <summary>Create an empty document.</summary>
    public CdcDocument()
    {
    }

    /// <summary>Create an empty document with the given initial capacity.</summary>
    public CdcDocument(int capacity) : base(capacity)
    {
    }

    /// <summary>Set a field and return the document, for fluent construction.</summary>
    public CdcDocument Set(string key, object? value)
    {
        this[key] = value;
        return this;
    }
}
