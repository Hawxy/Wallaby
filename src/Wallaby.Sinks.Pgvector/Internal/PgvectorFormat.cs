using System.Security.Cryptography;
using System.Text;

namespace Wallaby.Sinks.Pgvector.Internal;

/// <summary>Text hashing and vector extraction for the sink's write path.</summary>
internal static class PgvectorFormat
{
    /// <summary>
    /// The stored content hash: SHA-256 over the embedding version and the embedded text, so a model
    /// change re-embeds even when the text is unchanged.
    /// </summary>
    public static string TextHash(string version, string text)
        => Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes($"{version}\n{text}")));

    /// <summary>Extracts a vector value a transform placed in a document field.</summary>
    public static bool TryGetVector(object? value, out ReadOnlyMemory<float> vector)
    {
        switch (value)
        {
            case ReadOnlyMemory<float> memory:
                vector = memory;
                return true;
            case Memory<float> memory:
                vector = memory;
                return true;
            case float[] array:
                vector = array;
                return true;
            default:
                vector = default;
                return false;
        }
    }
}
