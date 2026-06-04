using Npgsql;

namespace Wallaby.Internal.State;

/// <summary>
/// Creates the internal <c>wallaby</c> schema and its bookkeeping tables (idempotently). State is
/// co-located in the source database so backfill watermarking and checkpointing observe a consistent
/// view of the data.
/// </summary>
internal sealed class StateSchemaBootstrapper
{
    private const string Ddl = """
        CREATE SCHEMA IF NOT EXISTS wallaby;

        CREATE TABLE IF NOT EXISTS wallaby.checkpoint (
            slot_name     text        PRIMARY KEY,
            confirmed_lsn pg_lsn      NOT NULL,
            updated_at    timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS wallaby.backfill_state (
            table_qualified   text        PRIMARY KEY,
            status            text        NOT NULL,
            transform_version text        NULL,
            cursor_json       jsonb       NULL,
            rows_copied       bigint      NOT NULL DEFAULT 0,
            updated_at        timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS wallaby.slot_registry (
            slot_name        text        PRIMARY KEY,
            publication      text        NOT NULL,
            consistent_point pg_lsn      NULL,
            kind             text        NOT NULL DEFAULT 'primary',
            created_at       timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS wallaby.fanout_queue (
            table_qualified text        NOT NULL,
            lookup_hash     text        NOT NULL,
            lookup_columns  text[]      NOT NULL,
            lookup_values   jsonb       NOT NULL,
            status          text        NOT NULL,
            cursor_json     jsonb       NULL,
            rows_copied     bigint      NOT NULL DEFAULT 0,
            requested_at    timestamptz NOT NULL DEFAULT now(),
            updated_at      timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (table_qualified, lookup_hash)
        );

        -- Disk-free spill for pgoutput v2 streamed transactions buffered until commit. 
        -- Used only by the database spill backend (PostgresUnloggedTableSpill);
        CREATE UNLOGGED TABLE IF NOT EXISTS wallaby.stream_buffer (
            slot_name text   NOT NULL,
            xid       bigint NOT NULL,
            seq       bigint NOT NULL,
            payload   bytea  NOT NULL,
            PRIMARY KEY (slot_name, xid, seq)
        );
        """;

    public Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct)
        => PgExec.ExecuteAsync(connection, Ddl, ct);
}
