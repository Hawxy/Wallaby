using System.Collections.Concurrent;
using System.Text.Json;
using EFCore.CDC.Abstractions;
using EFCore.CDC.Internal.Materialization;
using EFCore.CDC.Internal.State;
using EFCore.CDC.Model;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace EFCore.CDC.Internal.Backfill;

/// <summary>
/// Coordinates DBLog-style watermark backfill. The backfill task snapshots a table in keyset chunks,
/// bracketing each chunk with low/high watermark writes to <c>cdc.watermark</c>. The live pipeline (which
/// receives those watermarks through the same replication stream) records concurrent change keys between
/// the watermarks and emits the deduplicated snapshot rows at the high watermark — guaranteeing no gaps
/// and that live changes always win for overlapping keys.
/// </summary>
internal sealed class WatermarkBackfillCoordinator(
    string connectionString, IBackfillStateStore store, ILogger logger)
{
    private readonly ConcurrentDictionary<string, PendingWindow> _byLowToken = new();
    private readonly ConcurrentDictionary<string, PendingWindow> _byHighToken = new();
    private readonly ConcurrentDictionary<string, PendingWindow> _recordingByTable = new();

    public int ChunkSize { get; init; } = 500;

    // ---- backfill task side ----

    /// <summary>Snapshot a table chunk-by-chunk, resuming from persisted state. The live pipeline must be running.</summary>
    public async Task BackfillTableAsync(CapturedTable table, string? transformVersion, CancellationToken ct)
    {
        var pager = new KeysetPager(connectionString, table);
        var existing = await store.GetAsync(table.QualifiedName, ct);
        var cursor = DeserializeCursor(existing?.CursorJson, table);
        var rowsCopied = existing?.RowsCopied ?? 0;

        logger.LogInformation("Starting backfill of {Table}.", table.QualifiedName);

        while (!ct.IsCancellationRequested)
        {
            var window = new PendingWindow
            {
                QualifiedTable = table.QualifiedName,
                LowToken = Guid.NewGuid().ToString("N"),
                HighToken = Guid.NewGuid().ToString("N"),
            };
            _byLowToken[window.LowToken] = window;
            _byHighToken[window.HighToken] = window;

            await WriteWatermarkAsync(window.LowToken, ct);
            var chunk = await pager.ReadChunkAsync(cursor, ChunkSize, ct);
            window.Buffer = chunk.Rows;
            await WriteWatermarkAsync(window.HighToken, ct);

            await window.Completed.Task.WaitAsync(ct);

            rowsCopied += chunk.Rows.Count;
            cursor = chunk.NextCursor;
            var status = chunk.HasMore ? BackfillStatus.InProgress : BackfillStatus.Completed;
            await store.SaveAsync(
                new BackfillState(table.QualifiedName, status, transformVersion, SerializeCursor(cursor), rowsCopied, DateTimeOffset.UtcNow),
                ct);

            if (!chunk.HasMore)
            {
                break;
            }
        }

        logger.LogInformation("Backfill of {Table} complete ({Rows} rows).", table.QualifiedName, rowsCopied);
    }

    private async Task WriteWatermarkAsync(string token, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgExec.ExecuteAsync(connection, "UPDATE cdc.watermark SET token = @token WHERE id = 1", ct, ("token", token));
    }

    // ---- pipeline side ----

    public void OnLowWatermark(string token)
    {
        if (_byLowToken.TryGetValue(token, out var window))
        {
            _recordingByTable[window.QualifiedTable] = window;
        }
    }

    public void RecordLiveKey(string qualifiedTable, DocumentKey key)
    {
        if (_recordingByTable.TryGetValue(qualifiedTable, out var window))
        {
            window.SeenKeys.Add(key);
        }
    }

    public bool TryTakeHighWindow(string token, out PendingWindow window)
    {
        if (_byHighToken.TryRemove(token, out var found))
        {
            _byLowToken.TryRemove(found.LowToken, out _);
            _recordingByTable.TryRemove(new KeyValuePair<string, PendingWindow>(found.QualifiedTable, found));
            window = found;
            return true;
        }

        window = null!;
        return false;
    }

    // ---- cursor (de)serialization ----

    private static string? SerializeCursor(object?[]? cursor)
        => cursor is null ? null : JsonSerializer.Serialize(cursor);

    private static object?[]? DeserializeCursor(string? json, CapturedTable table)
    {
        if (string.IsNullOrEmpty(json))
        {
            return null;
        }

        var elements = JsonSerializer.Deserialize<JsonElement[]>(json);
        if (elements is null)
        {
            return null;
        }

        var cursor = new object?[elements.Length];
        for (var i = 0; i < elements.Length; i++)
        {
            cursor[i] = JsonElementToClr(elements[i], table.PrimaryKey[i].ClrType);
        }
        return cursor;
    }

    private static object? JsonElementToClr(JsonElement element, Type target)
    {
        var underlying = Nullable.GetUnderlyingType(target) ?? target;
        return element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True or JsonValueKind.False => element.GetBoolean(),
            JsonValueKind.String => ValueCoercion.ToClr(element.GetString(), target),
            JsonValueKind.Number when underlying == typeof(long) => element.GetInt64(),
            JsonValueKind.Number when underlying == typeof(int) || underlying == typeof(short) => element.GetInt32(),
            JsonValueKind.Number when underlying == typeof(decimal) => element.GetDecimal(),
            JsonValueKind.Number when underlying == typeof(double) || underlying == typeof(float) => element.GetDouble(),
            JsonValueKind.Number => element.GetInt64(),
            _ => ValueCoercion.ToClr(element.GetRawText(), target),
        };
    }
}
