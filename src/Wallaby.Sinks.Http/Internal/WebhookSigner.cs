using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Wallaby.Abstractions;
using Wallaby.Sinks;

namespace Wallaby.Sinks.Http.Internal;

/// <summary>
/// <a href="https://www.standardwebhooks.com/">Standard Webhooks</a> request signing for the sink:
/// parses the configured secrets, derives the content-stable message id, and produces the
/// <c>webhook-id</c> / <c>webhook-timestamp</c> / <c>webhook-signature</c> header values for one
/// request.
/// </summary>
internal sealed class WebhookSigner
{
    private const string SecretPrefix = "whsec_";

    // Standard Webhooks generates 24-byte secrets; 16 is the floor for an HMAC-SHA256 key.
    private const int MinKeyBytes = 16;

    private const string Guidance =
        "Generate one with: \"" + SecretPrefix + "\" + Convert.ToBase64String(RandomNumberGenerator.GetBytes(24)).";

    private readonly byte[] _key;
    private readonly byte[]? _previousKey;

    private WebhookSigner(byte[] key, byte[]? previousKey)
    {
        _key = key;
        _previousKey = previousKey;
    }

    /// <summary>Null when signing is not configured; throws on an unusable configuration.</summary>
    public static WebhookSigner? Create(string sinkName, HttpSinkOptions options)
    {
        var key = ParseKey(options.SigningSecret, nameof(options.SigningSecret));
        var previousKey = ParseKey(options.PreviousSigningSecret, nameof(options.PreviousSigningSecret));
        if (key is null)
        {
            if (previousKey is not null)
            {
                throw new WallabyConfigurationException(
                    $"HTTP sink '{sinkName}' sets {nameof(options.PreviousSigningSecret)} without " +
                    $"{nameof(options.SigningSecret)}; the previous secret only augments an active one " +
                    "during rotation.");
            }
            return null;
        }
        return new WebhookSigner(key, previousKey);
    }

    /// <summary>The three header values for one request over the given chunk and serialized body.</summary>
    public WebhookHeaders Sign(SinkBatch batch, int offset, int count, ReadOnlySpan<byte> body)
    {
        var id = MessageId(batch, offset, count);
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
        var signature = Sign(_key, id, timestamp, body);
        if (_previousKey is not null)
        {
            // One signature per active key, space-delimited per the specification.
            signature = $"{signature} {Sign(_previousKey, id, timestamp, body)}";
        }
        return new WebhookHeaders(id, timestamp, signature);
    }

    // Standard Webhooks secrets are base64, optionally 'whsec_'-prefixed. The key bytes are the decoded
    // base64, matching every spec verification library; a non-base64 secret would sign with different
    // key bytes than receivers derive, so it fails fast instead.
    private static byte[]? ParseKey(string? secret, string optionName)
    {
        if (secret is null)
        {
            return null;
        }
        var encoded = secret.StartsWith(SecretPrefix, StringComparison.Ordinal)
            ? secret[SecretPrefix.Length..]
            : secret;
        byte[] key;
        try
        {
            key = Convert.FromBase64String(encoded);
        }
        catch (FormatException ex)
        {
            throw new WallabyConfigurationException(
                $"HTTP sink {optionName} must be a Standard Webhooks secret: base64, optionally prefixed " +
                $"'{SecretPrefix}'. {Guidance}", ex);
        }
        if (key.Length < MinKeyBytes)
        {
            // An empty string (an unset environment variable binds to one) and a bare 'whsec_' both
            // decode to zero bytes, which would sign every request with an empty HMAC key.
            throw new WallabyConfigurationException(
                $"HTTP sink {optionName} decodes to {key.Length} key byte(s); a signing secret must carry " +
                $"at least {MinKeyBytes}. {Guidance}");
        }
        return key;
    }

    // The id hashes the chunk's record idempotency keys rather than the body: the envelope's sentAt
    // stamp makes body bytes differ per attempt, while the keys identify the delivered content and are
    // stable across retries.
    private static string MessageId(SinkBatch batch, int offset, int count)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(Encoding.UTF8.GetBytes(batch.SinkName));
        for (var i = offset; i < offset + count; i++)
        {
            hash.AppendData("\n"u8);
            hash.AppendData(Encoding.UTF8.GetBytes(SinkEnvelopeJson.IdempotencyKey(batch.Records[i])));
        }
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        hash.GetHashAndReset(digest);
        return $"msg_{Convert.ToHexStringLower(digest[..16])}";
    }

    /// <summary><c>v1,&lt;base64&gt;</c> HMAC-SHA256 over <c>{id}.{timestamp}.{body}</c>.</summary>
    private static string Sign(byte[] key, string id, string timestamp, ReadOnlySpan<byte> body)
    {
        using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, key);
        hmac.AppendData(Encoding.ASCII.GetBytes(id));
        hmac.AppendData("."u8);
        hmac.AppendData(Encoding.ASCII.GetBytes(timestamp));
        hmac.AppendData("."u8);
        hmac.AppendData(body);
        return $"v1,{Convert.ToBase64String(hmac.GetHashAndReset())}";
    }
}

/// <summary>The signing header values for one request.</summary>
internal readonly record struct WebhookHeaders(string Id, string Timestamp, string Signature);
