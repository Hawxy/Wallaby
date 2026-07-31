using System.IO.Compression;
using System.Text;
using System.Text.Json;
using StandardWebhooks;
using Wallaby.Abstractions;
using static Wallaby.Sinks.Http.Tests.Unit.SinkTestHelpers;

namespace Wallaby.Sinks.Http.Tests.Unit;

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
            o.SigningSecret = "whsec_dGVzdC1zaWduaW5nLWtleS0wMTIzNDU2Nzg5YWJjZGVm";
        });

        (await sink.DeliverAsync(OneRecord(), CancellationToken.None)).Status.ShouldBe(DeliveryStatus.Success);

        var captured = handler.Requests.ShouldHaveSingleItem();
        var expected = new StandardWebhook("whsec_dGVzdC1zaWduaW5nLWtleS0wMTIzNDU2Nzg5YWJjZGVm").Sign(
            captured.Id!,
            DateTimeOffset.FromUnixTimeSeconds(long.Parse(captured.Timestamp!)),
            Encoding.UTF8.GetString(Decompress(captured.Body, "gzip")));
        captured.Signature.ShouldBe(expected);
    }
}
