using Npgsql;
using NpgsqlTypes;

namespace Wallaby.Sinks.Pgvector.Internal;

/// <summary>
/// The sink's SQL surface: identifier policy, extension and table creation, stored-hash reads, and
/// the transactional upsert/delete write.
/// </summary>
internal sealed class PgvectorTables(string sinkName, NpgsqlDataSource dataSource, PgvectorSinkOptions options)
{
    private readonly HashSet<string> _ensured = [];

    public static bool IsValidIdentifier(string? value)
        => value is { Length: > 0 and <= 63 } && value.All(c => char.IsAsciiLetterOrDigit(c) || c == '_');

    public void RequireValidTable(string table)
    {
        if (!IsValidIdentifier(table))
        {
            throw new PermanentDeliveryException(
                $"Destination '{table}' is not a valid pgvector table name for sink '{sinkName}': use 1-63 " +
                "characters of [a-zA-Z0-9_].");
        }
    }

    public async Task EnsureExtensionAsync(CancellationToken ct)
    {
        try
        {
            await using var cmd = dataSource.CreateCommand("CREATE EXTENSION IF NOT EXISTS vector");
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.InsufficientPrivilege)
        {
            throw new WallabyConfigurationException(
                $"Pgvector sink '{sinkName}' cannot create the 'vector' extension (insufficient privilege). " +
                "Have a superuser run CREATE EXTENSION vector; on the destination database, or set " +
                "CreateExtension = false once it exists.", ex);
        }
        catch (PostgresException ex) when (
            ex.SqlState is PostgresErrorCodes.UniqueViolation or PostgresErrorCodes.DuplicateObject)
        {
            // IF NOT EXISTS is not concurrency-safe; a concurrent creator winning means it exists.
        }
        // The data source may have loaded its type catalog before the extension existed (its first
        // connection is this CREATE EXTENSION); reload so the 'vector' type resolves for parameters.
        await dataSource.ReloadTypesAsync(ct);
    }

    public async Task EnsureTableAsync(string table, CancellationToken ct)
    {
        lock (_ensured)
        {
            if (_ensured.Contains(table))
            {
                return;
            }
        }
        try
        {
            await using var cmd = dataSource.CreateCommand(
                $"""
                 CREATE TABLE IF NOT EXISTS {Qualified(table)} (
                     id         text PRIMARY KEY,
                     text_hash  text,
                     embedding  vector({options.Dimensions}),
                     document   jsonb NOT NULL,
                     updated_at timestamptz NOT NULL DEFAULT now()
                 )
                 """);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException ex) when (
            ex.SqlState is PostgresErrorCodes.UniqueViolation or PostgresErrorCodes.DuplicateTable)
        {
            // IF NOT EXISTS is not concurrency-safe; a concurrent creator winning means it exists.
        }
        lock (_ensured)
        {
            _ensured.Add(table);
        }
    }

    public async Task<Dictionary<string, string>> LoadStoredHashesAsync(
        string table, string[] ids, CancellationToken ct)
    {
        var hashes = new Dictionary<string, string>(StringComparer.Ordinal);
        // A hash only counts alongside a stored vector; a hash next to a null embedding (however it
        // arose) must re-embed rather than gate.
        await using var cmd = dataSource.CreateCommand(
            $"SELECT id, text_hash FROM {Qualified(table)} " +
            "WHERE id = ANY($1) AND text_hash IS NOT NULL AND embedding IS NOT NULL");
        cmd.Parameters.Add(new NpgsqlParameter { Value = ids });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            hashes[reader.GetString(0)] = reader.GetString(1);
        }
        return hashes;
    }

    public async Task WriteAsync(
        string table, IReadOnlyList<PgvectorRow> rows, IReadOnlyList<string> deletes, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        foreach (var chunk in rows.Chunk(options.MaxRowsPerBatch))
        {
            await using var writeBatch = new NpgsqlBatch(connection, transaction);
            foreach (var row in chunk)
            {
                // A KeepStoredVector row normally exists (its stored hash matched) and takes the update
                // arm leaving embedding and text_hash untouched - but only after re-verifying the hash
                // and vector under the row lock, so a row changed between the hash read and this write
                // (e.g. by a concurrent deliverer) is left intact rather than paired with a foreign
                // vector. If the row vanished instead, the insert arm writes the hash with a null
                // vector, which the hash read's embedding filter treats as absent, so the next delivery
                // re-embeds. Both arms guard with IS DISTINCT FROM so an identical redelivery does not
                // rewrite the tuple (no WAL or dead-tuple churn on re-backfills of unchanged rows).
                var update = row.KeepStoredVector
                    ? "document = EXCLUDED.document, updated_at = now() " +
                      "WHERE t.text_hash = EXCLUDED.text_hash AND t.embedding IS NOT NULL " +
                      "AND t.document IS DISTINCT FROM EXCLUDED.document"
                    : "text_hash = EXCLUDED.text_hash, embedding = EXCLUDED.embedding, " +
                      "document = EXCLUDED.document, updated_at = now() " +
                      "WHERE (t.text_hash, t.embedding, t.document) IS DISTINCT FROM " +
                      "(EXCLUDED.text_hash, EXCLUDED.embedding, EXCLUDED.document)";
                var cmd = new NpgsqlBatchCommand(
                    $"INSERT INTO {Qualified(table)} AS t (id, text_hash, embedding, document) " +
                    $"VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET {update}");
                cmd.Parameters.Add(new NpgsqlParameter { Value = row.Id });
                cmd.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Hash ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                // A Vector value infers the vector type via the plugin; a bare null is sent untyped and
                // the server infers it from the target column.
                cmd.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Vector ?? DBNull.Value });
                cmd.Parameters.Add(new NpgsqlParameter { Value = row.DocumentJson, NpgsqlDbType = NpgsqlDbType.Jsonb });
                writeBatch.BatchCommands.Add(cmd);
            }
            await writeBatch.ExecuteNonQueryAsync(ct);
        }
        if (deletes.Count > 0)
        {
            await using var delete = new NpgsqlCommand(
                $"DELETE FROM {Qualified(table)} WHERE id = ANY($1)", connection, transaction);
            delete.Parameters.Add(new NpgsqlParameter { Value = deletes.ToArray() });
            await delete.ExecuteNonQueryAsync(ct);
        }
        await transaction.CommitAsync(ct);
    }

    public async Task PurgeAsync(string table, CancellationToken ct)
    {
        try
        {
            await using var cmd = dataSource.CreateCommand($"DELETE FROM {Qualified(table)}");
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            // Nothing to purge; the table is created on initialization or first delivery.
        }
    }

    private string Qualified(string table) => $"\"{options.Schema}\".\"{table}\"";
}
