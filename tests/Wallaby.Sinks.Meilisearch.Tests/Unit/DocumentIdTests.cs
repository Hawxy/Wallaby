using System.Text.Json;
using Wallaby.Abstractions;
using static Wallaby.Sinks.Meilisearch.Tests.Unit.MeilisearchTestHelpers;

namespace Wallaby.Sinks.Meilisearch.Tests.Unit;

/// <summary>
/// Meilisearch document ids: readable when the canonical id already fits Meilisearch's alphabet, base64url
/// otherwise, never shared by two keys of one table, and always decodable back to the canonical id.
/// </summary>
public class DocumentIdTests
{
    [Test]
    [Arguments("42", "42")]
    [Arguments("-7", "-7")]
    [Arguments("3f2b8c1e-0000-4000-8000-000000000001", "3f2b8c1e-0000-4000-8000-000000000001")]
    [Arguments("order_line", "order_line")]
    [Arguments("1|23", "1_23")]
    [Arguments("tenant-a|3f2b8c1e-0000-4000-8000-000000000001", "tenant-a_3f2b8c1e-0000-4000-8000-000000000001")]
    public void Ids_in_the_allowed_alphabet_stay_readable(string documentId, string expected)
    {
        MeilisearchDocumentIds.Encode(documentId).ShouldBe(expected);
    }

    [Test]
    [Arguments("a.b@x.com")]
    [Arguments("café")]
    [Arguments("_leading")]
    [Arguments("")]
    [Arguments("a_b|c")]
    [Arguments("a|")]
    [Arguments("|a")]
    [Arguments("a||b")]
    [Arguments("a%7Cb")]
    public void Other_ids_are_base64url_encoded(string documentId)
    {
        var id = MeilisearchDocumentIds.Encode(documentId);

        id.ShouldStartWith("_e");
        id.ShouldMatch("^[A-Za-z0-9_-]*$");
    }

    [Test]
    [Arguments("a_b|c", "a|b_c")]
    [Arguments("acme.io|g", "acme_io|g")]
    [Arguments("日本", "中国")]
    [Arguments("a.b", "a_b")]
    [Arguments("_eYS5i", "a.b")] // the literal key vs the key whose fallback id it spells
    public void Keys_that_used_to_sanitize_alike_get_distinct_ids(string first, string second)
    {
        MeilisearchDocumentIds.Encode(first).ShouldNotBe(MeilisearchDocumentIds.Encode(second));
    }

    [Test]
    [Arguments("42", 1)]
    [Arguments("order_line", 1)]
    [Arguments("a.b@x.com", 1)]
    [Arguments("_eYQ", 1)]
    [Arguments("", 1)]
    [Arguments("1|23", 2)]
    [Arguments("a_b|c", 2)]
    [Arguments("x|y|z", 3)]
    public void Decode_reverses_encode(string documentId, int keyParts)
    {
        MeilisearchDocumentIds.Decode(MeilisearchDocumentIds.Encode(documentId), keyParts).ShouldBe(documentId);
    }

    [Test]
    public void Decode_rejects_a_readable_id_with_the_wrong_number_of_values()
    {
        Should.Throw<FormatException>(() => MeilisearchDocumentIds.Decode("1_23", keyParts: 3));
    }

    [Test]
    [Arguments("_x")]
    [Arguments("a.b")]
    public void Decode_rejects_ids_encode_never_produces(string id)
    {
        Should.Throw<FormatException>(() => MeilisearchDocumentIds.Decode(id, keyParts: 1));
    }

    [Test]
    public void Ids_over_the_length_limit_fail_instead_of_truncating()
    {
        MeilisearchDocumentIds.Encode(new string('a', MeilisearchDocumentIds.MaxLength)).Length
            .ShouldBe(MeilisearchDocumentIds.MaxLength);

        var ex = Should.Throw<MeilisearchDocumentIdException>(
            () => MeilisearchDocumentIds.Encode(new string('a', MeilisearchDocumentIds.MaxLength + 1)));
        ex.Message.ShouldContain("KeyedBy");
    }

    [Test]
    public async Task Upserts_and_deletions_use_the_encoded_id()
    {
        var stub = new StubHandler();
        var sink = Sink(stub);

        await sink.DeliverAsync(Batch(Upsert("a.b|1"), Delete("a_b|1")), CancellationToken.None);

        var posts = stub.Requests.Where(r => r.Method == HttpMethod.Post).ToList();
        var added = JsonDocument.Parse(await posts[0].Content!.ReadAsStringAsync()).RootElement[0];
        added.GetProperty("id").GetString().ShouldBe(MeilisearchDocumentIds.Encode("a.b|1"));
        var deleted = JsonDocument.Parse(await posts[1].Content!.ReadAsStringAsync()).RootElement[0];
        deleted.GetString().ShouldBe(MeilisearchDocumentIds.Encode("a_b|1"));
        deleted.GetString().ShouldNotBe(added.GetProperty("id").GetString());
    }

    [Test]
    public async Task An_overlong_id_fails_permanently()
    {
        var stub = new StubHandler();
        var sink = Sink(stub);

        var result = await sink.DeliverAsync(Batch(Upsert(new string('a', 600))), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        stub.Requests.ShouldBeEmpty();
    }

    [Test]
    public async Task The_encoded_id_replaces_a_primary_key_field_the_transform_emitted()
    {
        var stub = new StubHandler();
        var sink = Sink(stub);
        var record = new SinkRecord("products", "a.b", new WallabyDocument { ["id"] = "a.b" }, IsDeletion: false, Meta());

        var result = await sink.DeliverAsync(Batch(record), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.Success);
        var body = await stub.Requests.First(r => r.Method == HttpMethod.Post).Content!.ReadAsStringAsync();
        JsonDocument.Parse(body).RootElement[0].GetProperty("id").GetString().ShouldBe(MeilisearchDocumentIds.Encode("a.b"));
    }
}
