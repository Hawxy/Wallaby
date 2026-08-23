// NativeAOT smoke test: publishes with PublishAot and exercises the AOT-sensitive Wallaby paths at
// runtime — spilled-change codecs, keyset cursors, Marten capture-plan derivation, and document
// materialization through a source-generated System.Text.Json serializer. Exits non-zero on the first
// failed check, so a CI publish + run catches both ILC-time and runtime AOT regressions.
using System.Collections;
using System.Net;
using System.Net.NetworkInformation;
using System.Text.Json;
using System.Text.Json.Serialization;
using Marten;
using NpgsqlTypes;
using Wallaby.Abstractions;
using Wallaby.AotSmokeTest;
using Wallaby.Sinks;
using Wallaby.Sinks.Pgvector;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Replication;
using Wallaby.Providers.Marten.Internal;
using Wallaby.Model;
using Wallaby.Providers;
using Weasel.Core;

var failures = 0;

Check("spill codec round-trips every tagged type", () =>
{
    var guid = Guid.NewGuid();
    var utc = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
    var dto = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(5));

    // One column per spill tag: scalars, "a:" arrays, and "an:" nullable-element arrays. A value
    // slipping off its tag onto the reflection-JSON fallback fails this check under AOT.
    (string Name, object? Value)[] fixture =
    [
        ("null", null),
        ("b", true),
        ("u8", (byte)200),
        ("i16", (short)7),
        ("i32", 42),
        ("i64", 9_999_999_999L),
        ("u32", 4_000_000_000u),
        ("u64", 18_000_000_000_000_000_000ul),
        ("dec", 12.3400m),
        ("f64", 3.141592653589793d),
        ("f32", 1.5f),
        ("s", "kanga"),
        ("c", 'x'),
        ("g", guid),
        ("dt", utc),
        ("dto", dto),
        ("d", new DateOnly(2024, 1, 2)),
        ("t", new TimeOnly(3, 4, 5)),
        ("ts", TimeSpan.FromMinutes(90)),
        ("ip", IPAddress.Parse("2001:db8::1")),
        ("mac", PhysicalAddress.Parse("00-11-22-33-44-55")),
        ("bits", new BitArray(new[] { true, false, true })),
        ("bytes", new byte[] { 1, 2, 255 }),
        ("a_s", new[] { "a", null, "c" }),
        ("a_b", new[] { true, false }),
        ("a_i16", new short[] { 1, 2 }),
        ("a_i32", new[] { 1, 2 }),
        ("a_i64", new[] { 9_999_999_999L }),
        ("a_u32", new[] { 1u, 4_000_000_000u }),
        ("a_dec", new[] { 1.50m }),
        ("a_f64", new[] { 1.25 }),
        ("a_f32", new[] { 1.5f }),
        ("a_g", new[] { guid }),
        ("a_dt", new[] { utc }),
        ("a_dto", new[] { dto }),
        ("a_d", new[] { new DateOnly(2024, 1, 2) }),
        ("a_t", new[] { new TimeOnly(3, 4, 5) }),
        ("a_ts", new[] { TimeSpan.FromMinutes(1) }),
        ("a_ip", new[] { IPAddress.Loopback, null }),
        ("an_b", new bool?[] { true, null }),
        ("an_i16", new short?[] { 1, null }),
        ("an_i32", new int?[] { 1, null, 3 }),
        ("an_i64", new long?[] { null, 9_999_999_999L }),
        ("an_u32", new uint?[] { 1u, null }),
        ("an_dec", new decimal?[] { 1.50m, null }),
        ("an_f64", new double?[] { 1.25, null }),
        ("an_f32", new float?[] { 1.5f, null }),
        ("an_g", new Guid?[] { guid, null }),
        ("an_dt", new DateTime?[] { utc, null }),
        ("an_dto", new DateTimeOffset?[] { dto, null }),
        ("an_d", new DateOnly?[] { new DateOnly(2024, 1, 2), null }),
        ("an_t", new TimeOnly?[] { new TimeOnly(3, 4, 5), null }),
        ("an_ts", new TimeSpan?[] { TimeSpan.FromMinutes(1), null }),
    ];

    var change = new RawChange
    {
        RelationId = 1,
        Schema = "public",
        TableName = "t",
        Action = ChangeAction.Insert,
        NewValues =
        [
            .. fixture.Select(f => new RawColumn { ColumnName = f.Name, Value = f.Value }),
            new RawColumn { ColumnName = "toasted", IsUnchangedToast = true },
        ],
    };

    var r = SpillCodec.Decode(SpillCodec.Encode(change)).NewValues;

    for (var i = 0; i < fixture.Length; i++)
    {
        AssertValue(fixture[i].Value, r[i].Value, fixture[i].Name);
    }
    if (!r[fixture.Length].IsUnchangedToast) throw new InvalidOperationException("toast flag lost");
});

Check("spill fallback is guarded by reflection availability", () =>
{
    var change = new RawChange
    {
        RelationId = 1,
        Schema = "public",
        TableName = "t",
        Action = ChangeAction.Insert,
        NewValues = [new RawColumn { ColumnName = "exotic", Value = new NpgsqlPoint(1.5, -2.5) }],
    };

    if (JsonSerializer.IsReflectionEnabledByDefault)
    {
        // JIT/untrimmed host (e.g. dotnet run): the fallback round-trips.
        var r = SpillCodec.Decode(SpillCodec.Encode(change)).NewValues;
        AssertEqual(new NpgsqlPoint(1.5, -2.5), r[0].Value, "fallback point");
        return;
    }

    try
    {
        SpillCodec.Encode(change);
        throw new InvalidOperationException("expected NotSupportedException from the spill fallback");
    }
    catch (NotSupportedException)
    {
        // AOT host: the guarded fallback fails fast with guidance instead of an STJ internal error.
    }
});

Check("keyset cursor round-trips", () =>
{
    var id = Guid.NewGuid();
    var json = KeysetCodec.SerializeCursor([id, 5L], ["tenant_id", "id"]);
    if (!KeysetCodec.TryDeserializeCursor(json, ["tenant_id", "id"], [typeof(Guid), typeof(long)], out var cursor))
    {
        throw new InvalidOperationException("cursor rejected");
    }
    AssertEqual(id, cursor![0], "cursor guid");
    AssertEqual(5L, cursor[1], "cursor long");
});

Check("marten capture plan derives and materializes a document", () =>
{
    var options = new StoreOptions();
    options.Connection("Host=localhost;Database=db;Username=u;Password=p");
    options.DatabaseSchemaName = "docs";
    options.UseSystemTextJsonForSerialization(EnumStorage.AsInteger, Casing.Default,
        o => o.TypeInfoResolverChain.Insert(0, SmokeJsonContext.Default));
    options.RegisterDocumentType<SmokeDoc>();

    var plan = new MartenModelProvider(options).BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(SmokeDoc)] });

    var table = plan.Model.FindByClrType(typeof(SmokeDoc))
        ?? throw new InvalidOperationException("SmokeDoc table missing from the capture plan");
    AssertEqual("docs", table.Schema, "schema");
    AssertEqual("mt_doc_smokedoc", table.TableName, "table name");

    var doc = new SmokeDoc { Id = Guid.NewGuid(), Name = "roo" };
    var insert = new RawChange
    {
        RelationId = 1,
        Schema = table.Schema,
        TableName = table.TableName,
        Action = ChangeAction.Insert,
        NewValues =
        [
            new RawColumn { ColumnName = "id", Value = doc.Id },
            new RawColumn { ColumnName = "data", Value = JsonSerializer.Serialize(doc, SmokeJsonContext.Default.SmokeDoc) },
        ],
    };

    if (!plan.Materializer.TryMaterialize(insert, out var row))
    {
        throw new InvalidOperationException("insert was not materialized");
    }
    var entity = (SmokeDoc)row.Entity!;
    AssertEqual(doc.Id, entity.Id, "materialized id");
    AssertEqual("roo", entity.Name, "materialized name");
    AssertEqual(doc.Id, row.PrimaryKey[0], "primary key");
});

Check("vector documents serialize reflection-free and the pgvector sink constructs", () =>
{
    var document = new WallabyDocument { ["name"] = "roo", ["embedding"] = new ReadOnlyMemory<float>([3f, 1f]) };
    using var stream = new MemoryStream();
    using (var writer = new Utf8JsonWriter(stream))
    {
        SinkEnvelopeJson.WriteDocument(writer, document, "1", serializerOptions: null);
    }
    var json = System.Text.Encoding.UTF8.GetString(stream.ToArray());
    if (!json.Contains("\"embedding\":[3,1]"))
    {
        throw new InvalidOperationException($"vector not serialized as a number array: {json}");
    }

    // Construction validates options and builds the data source without connecting.
    var sink = new PgvectorSink("smoke", new PgvectorSinkOptions
    {
        ConnectionString = "Host=localhost;Database=vectors;Username=u;Password=p",
        Dimensions = 2,
        DefaultTable = "documents",
    });
    sink.DisposeAsync().AsTask().GetAwaiter().GetResult();
});

Console.WriteLine(failures == 0 ? "AOT smoke: all checks passed." : $"AOT smoke: {failures} check(s) FAILED.");
return failures == 0 ? 0 : 1;

void Check(string name, Action body)
{
    try
    {
        body();
        Console.WriteLine($"  ok: {name}");
    }
    catch (Exception ex)
    {
        failures++;
        Console.Error.WriteLine($"FAIL: {name}: {ex}");
    }
}

static void AssertEqual(object? expected, object? actual, string what)
{
    if (!Equals(expected, actual))
    {
        throw new InvalidOperationException($"{what}: expected '{expected}', got '{actual}'");
    }
}

static void AssertSequence<T>(IEnumerable<T> expected, IEnumerable<T> actual, string what)
{
    if (!expected.SequenceEqual(actual))
    {
        throw new InvalidOperationException($"{what}: sequences differ");
    }
}

// CLR-type fidelity matters (the materializer coerces from exactly what the decoder produced), so the
// runtime type must survive alongside the value; arrays and BitArray compare element-wise.
static void AssertValue(object? expected, object? actual, string what)
{
    if (expected is null)
    {
        if (actual is not null) throw new InvalidOperationException($"{what}: expected null, got '{actual}'");
        return;
    }
    if (actual?.GetType() != expected.GetType())
    {
        throw new InvalidOperationException(
            $"{what}: expected type {expected.GetType()}, got {actual?.GetType().ToString() ?? "null"}");
    }
    if (expected is IEnumerable expectedItems and not string)
    {
        AssertSequence(expectedItems.Cast<object?>(), ((IEnumerable)actual!).Cast<object?>(), what);
        return;
    }
    AssertEqual(expected, actual, what);
}

namespace Wallaby.AotSmokeTest
{
    public sealed class SmokeDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    [JsonSerializable(typeof(SmokeDoc))]
    public sealed partial class SmokeJsonContext : JsonSerializerContext;
}
