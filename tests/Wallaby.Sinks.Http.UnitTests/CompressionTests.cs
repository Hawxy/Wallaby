using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Wallaby.Abstractions;
using static Wallaby.Sinks.Http.UnitTests.SinkTestHelpers;

namespace Wallaby.Sinks.Http.UnitTests;

/// <summary>Request-body compression: Content-Encoding, round-trip, and signing over the uncompressed payload.</summary>
public class CompressionTests
{
    private static SinkBatch OneRecord()
        => Batch(Upsert("1", new Dictionary<string, object?> { ["name"] = "Kangaroo" }));

    private static byte[] Decompress(byte[] body, string encoding)
    {
        using var input = new MemoryStream(body);
        using Stream decompressor = encoding == "gzip"
            ? new GZipStream(input, CompressionMode.Decompress)
            : new BrotliStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        decompressor.CopyTo(output);
        return output.ToArray();
    }

    [Test]
    [Arguments(HttpSinkCompression.Gzip, "gzip")]
    [Arguments(HttpSinkCompression.Brotli, "br")]
    public async Task Compressed_body_carries_the_encoding_and_round_trips(HttpSinkCompression compression, string encoding)
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler, o => o.Compression = compression);

        (await sink.DeliverAsync(OneRecord(), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        var captured = handler.Requests.ShouldHaveSingleItem();
        captured.ContentEncoding.ShouldBe(encoding);

        using var envelope = JsonDocument.Parse(Decompress(captured.Body, encoding));
        envelope.RootElement.GetProperty("records")[0]
            .GetProperty("document").GetProperty("name").GetString().ShouldBe("Kangaroo");
    }

    [Test]
    public async Task Uncompressed_requests_have_no_content_encoding()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler);

        (await sink.DeliverAsync(OneRecord(), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        handler.Requests.ShouldHaveSingleItem().ContentEncoding.ShouldBeNull();
    }

    [Test]
    public async Task Signature_covers_the_uncompressed_payload()
    {
        var handler = new CapturingHandler();
        var sink = CreateSink(handler, o =>
        {
            o.Compression = HttpSinkCompression.Gzip;
            o.SigningSecret = "s3cret";
        });

        (await sink.DeliverAsync(OneRecord(), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        var captured = handler.Requests.ShouldHaveSingleItem();
        var payload = Decompress(captured.Body, "gzip");
        var expected = Convert.ToHexStringLower(HMACSHA256.HashData(Encoding.UTF8.GetBytes("s3cret"), payload));
        captured.Signature.ShouldBe($"sha256={expected}");
    }
}
