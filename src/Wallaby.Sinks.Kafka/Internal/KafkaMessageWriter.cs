using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Wallaby.Abstractions;

namespace Wallaby.Sinks.Kafka.Internal;

/// <summary>
/// Serializes one <see cref="SinkRecord"/> into a Kafka message value (a self-contained JSON envelope)
/// and its headers. The record-level pieces (metadata, document values, idempotency key) come from
/// <see cref="SinkEnvelopeJson"/>; this class owns the message shape and headers. Deletions carry no
/// value (a tombstone), so their context lives in the headers.
/// </summary>
internal static class KafkaMessageWriter
{
    /// <summary>Header carrying <c>upsert</c> or <c>delete</c>.</summary>
    public const string OperationHeader = "wallaby.operation";

    /// <summary>Header carrying the per-change deduplication key (see <see cref="IdempotencyKey"/>).</summary>
    public const string IdempotencyKeyHeader = "wallaby.idempotency-key";

    /// <summary>Header carrying the schema-qualified source table.</summary>
    public const string TableHeader = "wallaby.table";

    /// <summary>Header carrying the commit LSN (decimal string; <c>0</c> for backfill reads).</summary>
    public const string CommitLsnHeader = "wallaby.commit-lsn";

    private static readonly byte[] DeleteOperation = Encoding.UTF8.GetBytes("delete");
    private static readonly byte[] UpsertOperation = Encoding.UTF8.GetBytes("upsert");

    /// <summary>Headers for any record; present on tombstones too, where they are the only metadata.</summary>
    public static Headers BuildHeaders(SinkRecord record) => new()
    {
        { OperationHeader, record.IsDeletion ? DeleteOperation : UpsertOperation },
        { IdempotencyKeyHeader, Encoding.UTF8.GetBytes(IdempotencyKey(record)) },
        { TableHeader, Encoding.UTF8.GetBytes(record.Metadata.QualifiedTableName) },
        { CommitLsnHeader, Encoding.UTF8.GetBytes(record.Metadata.CommitLsn.ToString(CultureInfo.InvariantCulture)) },
    };

    /// <summary>Write an upsert's message value.</summary>
    public static byte[] WriteValue(
        SinkRecord record,
        IReadOnlyDictionary<string, string>? annotations,
        JsonSerializerOptions? serializerOptions)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer);

        writer.WriteStartObject();
        writer.WriteString("operation", "upsert");
        writer.WriteString("id", record.DocumentId);
        writer.WriteString("idempotencyKey", IdempotencyKey(record));
        if (annotations is { Count: > 0 })
        {
            writer.WriteStartObject("annotations");
            foreach (var annotation in annotations)
            {
                writer.WriteString(annotation.Key, annotation.Value);
            }
            writer.WriteEndObject();
        }

        writer.WritePropertyName("document");
        SinkEnvelopeJson.WriteDocument(writer, record.Document!, record.DocumentId, serializerOptions);

        SinkEnvelopeJson.WriteMetadata(writer, record.Metadata);
        writer.WriteEndObject();

        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    /// <inheritdoc cref="SinkEnvelopeJson.IdempotencyKey"/>
    public static string IdempotencyKey(SinkRecord record) => SinkEnvelopeJson.IdempotencyKey(record);
}
