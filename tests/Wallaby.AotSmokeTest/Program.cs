// NativeAOT smoke test: publishes with PublishAot and exercises the AOT-sensitive Wallaby paths at
// runtime — spilled-change codecs, keyset cursors, Marten capture-plan derivation, and document
// materialization through a source-generated System.Text.Json serializer. Exits non-zero on the first
// failed check, so a CI publish + run catches both ILC-time and runtime AOT regressions.
using System.Text.Json;
using System.Text.Json.Serialization;
using Marten;
using Wallaby.Abstractions;
using Wallaby.AotSmokeTest;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Replication;
using Wallaby.Marten.Internal;
using Wallaby.Model;
using Wallaby.Providers;
using Weasel.Core;

var failures = 0;

Check("spill codec round-trips scalars and arrays", () =>
{
    var guid = Guid.NewGuid();
    var change = new RawChange
    {
        RelationId = 1,
        Schema = "public",
        TableName = "t",
        Action = ChangeAction.Insert,
        NewValues =
        [
            new RawColumn { ColumnName = "id", Value = guid },
            new RawColumn { ColumnName = "count", Value = 42 },
            new RawColumn { ColumnName = "name", Value = "kanga" },
            new RawColumn { ColumnName = "tags", Value = new[] { "a", null, "c" } },
            new RawColumn { ColumnName = "nums", Value = new[] { 1L, 2L } },
            new RawColumn { ColumnName = "toasted", IsUnchangedToast = true },
        ],
    };

    var r = SpillCodec.Decode(SpillCodec.Encode(change)).NewValues;

    AssertEqual(guid, r[0].Value, "guid");
    AssertEqual(42, r[1].Value, "int");
    AssertEqual("kanga", r[2].Value, "string");
    AssertSequence(new[] { "a", null, "c" }, (string?[])r[3].Value!, "string array");
    AssertSequence(new long?[] { 1L, 2L }, ((long[])r[4].Value!).Cast<long?>(), "long array");
    if (!r[5].IsUnchangedToast) throw new InvalidOperationException("toast flag lost");
});

Check("spill fallback is guarded by reflection availability", () =>
{
    var change = new RawChange
    {
        RelationId = 1,
        Schema = "public",
        TableName = "t",
        Action = ChangeAction.Insert,
        NewValues = [new RawColumn { ColumnName = "exotic", Value = new int?[] { 1, null } }],
    };

    if (JsonSerializer.IsReflectionEnabledByDefault)
    {
        // JIT/untrimmed host (e.g. dotnet run): the fallback round-trips.
        var r = SpillCodec.Decode(SpillCodec.Encode(change)).NewValues;
        AssertSequence(new int?[] { 1, null }, (int?[])r[0].Value!, "fallback array");
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
