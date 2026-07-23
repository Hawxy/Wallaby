using BenchmarkDotNet.Attributes;
using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Model;

namespace Wallaby.Benchmarks;

/// <summary>
/// Encode/decode cost of the spill payload codec for a representative mixed-type row.
/// Baseline for any future binary-format proposal.
/// </summary>
[MemoryDiagnoser]
public class SpillCodecBenchmarks
{
    private RawChange _change = null!;
    private byte[] _encoded = null!;

    [GlobalSetup]
    public void Setup()
    {
        _change = new RawChange
        {
            RelationId = 42,
            Schema = "public",
            TableName = "products",
            Action = ChangeAction.Update,
            NewValues =
            [
                new RawColumn { ColumnName = "id", Value = Guid.NewGuid() },
                new RawColumn { ColumnName = "name", Value = "a mid-sized product name" },
                new RawColumn { ColumnName = "price", Value = 1234.56m },
                new RawColumn { ColumnName = "quantity", Value = 42 },
                new RawColumn { ColumnName = "updated_at", Value = DateTime.UtcNow },
                new RawColumn { ColumnName = "active", Value = true },
                new RawColumn { ColumnName = "payload", Value = new byte[64] },
            ],
        };
        _encoded = SpillCodec.Encode(_change);
    }

    [Benchmark]
    public byte[] Encode() => SpillCodec.Encode(_change);

    [Benchmark]
    public RawChange Decode() => SpillCodec.Decode(_encoded);
}
