using System.Buffers;
using System.Text.Json;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.OpenSearch.Internal;

/// <summary>
/// Serializes a slice of a <see cref="SinkBatch"/> into an NDJSON <c>_bulk</c> body: an action line
/// (<c>index</c>/<c>delete</c> with <c>_index</c> and <c>_id</c>) followed by the document line for upserts.
/// Document values are written by <see cref="SinkEnvelopeJson"/>.
/// </summary>
internal static class BulkWriter
{
    private const string SerializerOptionsName = "OpenSearchSinkOptions.SerializerOptions";

    private static readonly byte[] NewLine = "\n"u8.ToArray();

    /// <summary>Write records <paramref name="offset"/>..<paramref name="offset"/>+<paramref name="count"/> as one bulk body.</summary>
    public static byte[] Write(
        string sinkName,
        IReadOnlyList<SinkRecord> records,
        int offset,
        int count,
        string? defaultIndex,
        JsonSerializerOptions? serializerOptions)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer);

        for (var i = offset; i < offset + count; i++)
        {
            var record = records[i];
            var index = record.Destination ?? defaultIndex
                ?? throw new InvalidOperationException(
                    $"Record {record.DocumentId} has no destination and no DefaultIndex is configured for sink '{sinkName}'.");

            WriteAction(writer, buffer, record, index);
            if (!record.IsDeletion)
            {
                SinkEnvelopeJson.WriteDocument(writer, record.Document!, record.DocumentId, serializerOptions, SerializerOptionsName);
                EndLine(writer, buffer);
            }
        }

        return buffer.WrittenSpan.ToArray();
    }

    private static void WriteAction(Utf8JsonWriter writer, ArrayBufferWriter<byte> buffer, SinkRecord record, string index)
    {
        writer.WriteStartObject();
        writer.WriteStartObject(record.IsDeletion ? "delete" : "index");
        writer.WriteString("_index", index);
        writer.WriteString("_id", record.DocumentId);
        writer.WriteEndObject();
        writer.WriteEndObject();
        EndLine(writer, buffer);
    }

    /// <summary>Commit the current JSON line, append the NDJSON newline, and reset for the next line.</summary>
    private static void EndLine(Utf8JsonWriter writer, ArrayBufferWriter<byte> buffer)
    {
        writer.Flush();
        buffer.Write(NewLine);
        writer.Reset();
    }
}
