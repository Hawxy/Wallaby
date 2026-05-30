using Npgsql;

namespace EFCore.CDC.Internal.State;

/// <summary>
/// Creates the internal <c>cdc</c> schema and its bookkeeping tables (idempotently). State is
/// co-located in the source database so backfill watermarking and checkpointing observe a consistent
/// view of the data.
/// </summary>
internal sealed class StateSchemaBootstrapper
{
    private const string Ddl = """
        CREATE SCHEMA IF NOT EXISTS cdc;

        CREATE TABLE IF NOT EXISTS cdc.checkpoint (
            slot_name     text        PRIMARY KEY,
            confirmed_lsn pg_lsn      NOT NULL,
            updated_at    timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS cdc.backfill_state (
            table_qualified   text        PRIMARY KEY,
            status            text        NOT NULL,
            transform_version text        NULL,
            cursor_json       jsonb       NULL,
            rows_copied       bigint      NOT NULL DEFAULT 0,
            updated_at        timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS cdc.slot_registry (
            slot_name        text        PRIMARY KEY,
            publication      text        NOT NULL,
            consistent_point pg_lsn      NULL,
            created_at       timestamptz NOT NULL DEFAULT now()
        );
        """;

    public Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct)
        => PgExec.ExecuteAsync(connection, Ddl, ct);
}
