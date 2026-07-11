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

        -- Serves the worker's due-job scan: WHERE status IN (...) ORDER BY requested_at LIMIT 1.
        CREATE INDEX IF NOT EXISTS fanout_queue_due_idx
            ON wallaby.fanout_queue (requested_at)
            WHERE status IN ('Requested', 'InProgress');

        -- Disk-free spill for pgoutput v2 streamed transactions buffered until commit. 
        -- Used only by the database spill backend (PostgresUnloggedTableSpill);
        CREATE UNLOGGED TABLE IF NOT EXISTS wallaby.stream_buffer (
            slot_name text   NOT NULL,
            xid       bigint NOT NULL,
            subxid    bigint NOT NULL DEFAULT 0,
            seq       bigint NOT NULL,
            payload   bytea  NOT NULL,
            PRIMARY KEY (slot_name, xid, seq)
        );

        -- CREATE IF NOT EXISTS won't evolve an existing table; stale rows are cleared at session start.
        ALTER TABLE wallaby.stream_buffer ADD COLUMN IF NOT EXISTS subxid bigint NOT NULL DEFAULT 0;

        -- Finished fan-out jobs are deleted on completion; clear rows written before that behavior.
        DELETE FROM wallaby.fanout_queue WHERE status = 'Completed';
        """;

    public Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct)
        => PgExec.ExecuteAsync(connection, Ddl, ct);
}
