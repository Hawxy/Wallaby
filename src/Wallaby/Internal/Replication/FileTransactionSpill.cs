using System.Buffers.Binary;
using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Internal.Replication;

/// <summary>
/// Disk-backed <see cref="ITransactionSpill"/>: one append-only file per xid under a directory, each change
/// written as a length-prefixed <see cref="SpillCodec"/> record. The truest memory bound and zero source-DB
/// load, but it needs a writable path (mount a volume in locked-down containers). The default backend.
/// </summary>
internal sealed class FileTransactionSpill : ITransactionSpill
{
    private readonly string _directory;
    private readonly Dictionary<uint, FileStream> _writers = [];

    // First append offset per (xid, subxid) — the truncation point when that subtransaction aborts.
    // In-memory only: spill files never survive a session (ClearAsync runs at leader start).
    private readonly Dictionary<uint, Dictionary<uint, long>> _subxidFirstOffsets = [];

    public FileTransactionSpill(string directory)
    {
        _directory = directory;
        Directory.CreateDirectory(_directory);
    }

    private string PathFor(uint xid) => Path.Combine(_directory, $"{xid}.spill");

    public async ValueTask AppendAsync(uint xid, uint subxid, RawChange change, CancellationToken ct)
    {
        if (!_writers.TryGetValue(xid, out var writer))
        {
            writer = new FileStream(PathFor(xid), FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024, useAsync: true);
            _writers[xid] = writer;
        }

        if (!_subxidFirstOffsets.TryGetValue(xid, out var offsets))
        {
            offsets = [];
            _subxidFirstOffsets[xid] = offsets;
        }
        offsets.TryAdd(subxid, writer.Position);

        var payload = SpillCodec.Encode(change);
        var length = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, payload.Length);
        await writer.WriteAsync(length, ct);
        await writer.WriteAsync(payload, ct);
    }

    public async IAsyncEnumerable<RawChange> ReadAsync(
        uint xid, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        _subxidFirstOffsets.Remove(xid);
        if (_writers.Remove(xid, out var writer))
        {
            await writer.DisposeAsync();   // flush + close before reading back
        }

        var path = PathFor(xid);
        if (!File.Exists(path))
        {
            yield break;
        }

        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None, 64 * 1024, useAsync: true);
        var lengthBuffer = new byte[4];
        while (true)
        {
            if (!await ReadExactlyAsync(stream, lengthBuffer, ct))
            {
                yield break; // clean EOF
            }
            var length = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);
            var payload = new byte[length];
            if (!await ReadExactlyAsync(stream, payload, ct))
            {
                yield break; // truncated tail (e.g. a partially-written record after a crash) — stop cleanly
            }
            yield return SpillCodec.Decode(payload);
        }
    }

    public async ValueTask DiscardAsync(uint xid, CancellationToken ct)
    {
        _subxidFirstOffsets.Remove(xid);
        if (_writers.Remove(xid, out var writer))
        {
            await writer.DisposeAsync();
        }
        Delete(PathFor(xid));
    }

    public async ValueTask DiscardSubtransactionAsync(uint xid, uint subxid, CancellationToken ct)
    {
        // No recorded first offset means the subtransaction spilled nothing (or no spill exists at all).
        if (!_subxidFirstOffsets.TryGetValue(xid, out var offsets)
            || !offsets.TryGetValue(subxid, out var firstOffset)
            || !_writers.TryGetValue(xid, out var writer))
        {
            return;
        }

        await writer.FlushAsync(ct);
        writer.SetLength(firstOffset);  // Position clamps to the new length; later appends continue from here

        // Everything at/after the truncation point belonged to the aborted subtransaction or one nested in it.
        foreach (var (sub, offset) in offsets)
        {
            if (offset >= firstOffset)
            {
                offsets.Remove(sub);
            }
        }
    }

    public ValueTask ClearAsync(CancellationToken ct)
    {
        foreach (var writer in _writers.Values)
        {
            writer.Dispose();
        }
        _writers.Clear();
        _subxidFirstOffsets.Clear();
        if (Directory.Exists(_directory))
        {
            foreach (var file in Directory.EnumerateFiles(_directory, "*.spill"))
            {
                Delete(file);
            }
        }
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var writer in _writers.Values)
        {
            await writer.DisposeAsync();
        }
        _writers.Clear();
        _subxidFirstOffsets.Clear();
    }

    private static async Task<bool> ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        var read = 0;
        while (read < buffer.Length)
        {
            var n = await stream.ReadAsync(buffer.AsMemory(read), ct);
            if (n == 0)
            {
                return read == 0 ? false : throw new EndOfStreamException("Truncated spill record.");
            }
            read += n;
        }
        return true;
    }

    private static void Delete(string path)
    {
        try { File.Delete(path); }
        catch (FileNotFoundException) { }
        catch (DirectoryNotFoundException) { }
    }
}
