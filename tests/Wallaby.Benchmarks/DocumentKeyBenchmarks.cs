using BenchmarkDotNet.Attributes;
using Wallaby.Abstractions;

namespace Wallaby.Benchmarks;

/// <summary>
/// Hash/lookup cost of <see cref="DocumentKey"/> as a dictionary key: the router's per-change
/// collapse and transform-output probes.
/// </summary>
[MemoryDiagnoser]
public class DocumentKeyBenchmarks
{
    private DocumentKey _singleKey = null!;
    private DocumentKey _compositeKey = null!;
    private Dictionary<DocumentKey, int> _map = null!;

    [GlobalSetup]
    public void Setup()
    {
        _singleKey = new DocumentKey(123);
        _compositeKey = new DocumentKey(["tenant-1", 123L, Guid.NewGuid()]);
        _map = new Dictionary<DocumentKey, int>
        {
            [new DocumentKey(123)] = 1,
            [new DocumentKey(["tenant-1", 123L, Guid.NewGuid()])] = 2,
        };
    }

    [Benchmark]
    public int HashSingle() => _singleKey.GetHashCode();

    [Benchmark]
    public int HashComposite() => _compositeKey.GetHashCode();

    [Benchmark]
    public bool LookupSingle() => _map.TryGetValue(_singleKey, out _);
}
