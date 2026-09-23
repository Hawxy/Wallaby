using System.Buffers;
using System.Buffers.Text;
using System.Text;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Meilisearch;

/// <summary>
/// Converts between canonical document ids (<see cref="SinkRecord.DocumentId"/>, see
/// <see cref="DocumentKey.ToString"/>) and Meilisearch document ids, which allow only <c>[a-zA-Z0-9-_]</c>
/// and at most 511 bytes. The mapping is reversible and never gives two keys of one table the same id:
/// <list type="bullet">
/// <item>A single-value id of allowed characters that does not start with <c>_</c> is used as-is.</item>
/// <item>A composite id whose values all match <c>[a-zA-Z0-9-]+</c> joins them with <c>_</c>.</item>
/// <item>Any other id becomes <c>_e</c> followed by the unpadded base64url encoding of its UTF-8 bytes.</item>
/// </list>
/// </summary>
public static class MeilisearchDocumentIds
{
    /// <summary>The longest Meilisearch document id, in bytes (every encoded id is ASCII).</summary>
    public const int MaxLength = 511;

    private const string EncodedPrefix = "_e";
    private const string Alphanumerics = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static readonly SearchValues<char> IdChars = SearchValues.Create(Alphanumerics + "-_");
    private static readonly SearchValues<char> CompositeChars = SearchValues.Create(Alphanumerics + "-|");

    /// <summary>Encodes a canonical document id as a Meilisearch document id.</summary>
    /// <param name="documentId">A canonical document id.</param>
    /// <exception cref="MeilisearchDocumentIdException">The encoded id exceeds <see cref="MaxLength"/>.</exception>
    public static string Encode(string documentId)
    {
        ArgumentNullException.ThrowIfNull(documentId);

        var id = Readable(documentId)
            ?? EncodedPrefix + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(documentId));
        if (id.Length > MaxLength)
        {
            throw new MeilisearchDocumentIdException(documentId,
                $"Document '{Truncate(documentId)}' cannot be stored in Meilisearch: its encoded id is {id.Length} " +
                $"characters, over the {MaxLength}-byte limit. Use KeyedBy(...) to derive a shorter document id.");
        }
        return id;
    }

    /// <summary>
    /// Decodes a Meilisearch document id produced by <see cref="Encode"/> back to the canonical document id.
    /// Pass the result to <see cref="DocumentKey.SplitId"/> for the individual key values.
    /// </summary>
    /// <param name="id">A Meilisearch document id.</param>
    /// <param name="keyParts">The number of values in the source table's key (1 unless the key is composite).</param>
    /// <exception cref="FormatException">The id was not produced by <see cref="Encode"/> for a key of that size.</exception>
    public static string Decode(string id, int keyParts)
    {
        ArgumentNullException.ThrowIfNull(id);
        ArgumentOutOfRangeException.ThrowIfLessThan(keyParts, 1);

        string documentId;
        if (id.StartsWith(EncodedPrefix, StringComparison.Ordinal))
        {
            documentId = Encoding.UTF8.GetString(Base64Url.DecodeFromChars(id.AsSpan(EncodedPrefix.Length)));
        }
        else if (keyParts == 1)
        {
            documentId = id;
        }
        else if (id.AsSpan().Count('_') == keyParts - 1)
        {
            documentId = id.Replace('_', '|');
        }
        else
        {
            throw new FormatException($"'{id}' does not hold a key of {keyParts} values.");
        }

        // Only ids Encode produces decode, so the rules live in one place.
        if (id.Length > MaxLength || Encode(documentId) != id)
        {
            throw new FormatException($"'{id}' is not a Meilisearch document id produced by Wallaby.");
        }
        return documentId;
    }

    private static string? Readable(string documentId)
    {
        var span = documentId.AsSpan();
        if (span.Length == 0)
        {
            return null;
        }

        if (!span.Contains('|'))
        {
            return span[0] != '_' && !span.ContainsAnyExcept(IdChars) ? documentId : null;
        }

        // Every value must be non-empty so the '_'-joined form splits back into the same number of values.
        var valid = !span.ContainsAnyExcept(CompositeChars)
            && span[0] != '|' && span[^1] != '|' && !span.Contains("||", StringComparison.Ordinal);
        return valid ? documentId.Replace('|', '_') : null;
    }

    private static string Truncate(string documentId)
        => documentId.Length <= 64 ? documentId : documentId[..64] + "...";
}
