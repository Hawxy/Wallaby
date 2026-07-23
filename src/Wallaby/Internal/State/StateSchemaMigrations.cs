namespace Wallaby.Internal.State;

/// <summary>
/// The ordered migration steps for the internal <c>wallaby</c> state schema. Applied by
/// <see cref="StateSchemaBootstrapper"/>, which records each applied step in
/// <c>wallaby.schema_version</c> and refuses to run against a schema newer than
/// <see cref="CurrentVersion"/>.
/// </summary>
/// <remarks>
/// Rules for authoring a new step:
/// <list type="bullet">
/// <item>Append only, with the next version number — never edit a shipped step.</item>
/// <item>Steps must be idempotent (<c>IF NOT EXISTS</c> / <c>ADD COLUMN IF NOT EXISTS</c>): a step
/// interrupted before its version stamp re-applies cleanly.</item>
/// <item>New columns must carry <c>NOT NULL DEFAULT ...</c> — all host and client SQL uses explicit
/// column lists, so defaults keep older writers working during rolling upgrades.</item>
/// <item>Never rename columns: the remote client tolerates missing tables (42P01) but not missing
/// columns, and the host's binary COPY into <c>stream_buffer</c> is position-sensitive.</item>
/// <item><c>wallaby.stream_buffer</c> is UNLOGGED scratch space cleared at session start; it may be
/// dropped and recreated in a step.</item>
/// </list>
/// </remarks>
internal static class StateSchemaMigrations
{
    /// <summary>The schema version this build requires; the highest version in <see cref="Steps"/>.</summary>
    public const int CurrentVersion = 1;

    /// <summary>
    /// Baseline: the full schema as deployed by the 1.0.0 betas. Databases bootstrapped by those betas
    /// (which had no version ledger) adopt it as a no-op and get stamped version 1.
    /// </summary>
    private const string Baseline = """
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
            purge             boolean     NOT NULL DEFAULT false,
            updated_at        timestamptz NOT NULL DEFAULT now()
        );

        -- CREATE IF NOT EXISTS won't evolve a table created by an early beta.
        ALTER TABLE wallaby.backfill_state ADD COLUMN IF NOT EXISTS purge boolean NOT NULL DEFAULT false;

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

        ALTER TABLE wallaby.stream_buffer ADD COLUMN IF NOT EXISTS subxid bigint NOT NULL DEFAULT 0;

        -- Suspend/resume control row (singleton; wire format shared with Wallaby.Client via
        -- ControlContract). Created only by the host — the remote client never performs DDL.
        CREATE TABLE IF NOT EXISTS wallaby.control (
            scope        text        PRIMARY KEY DEFAULT 'wallaby' CHECK (scope = 'wallaby'),
            state        text        NOT NULL DEFAULT 'Running',
            origin       text        NOT NULL DEFAULT 'client',
            reason       text        NULL,
            requested_by text        NULL,
            requested_at timestamptz NULL,
            suspended_at timestamptz NULL,
            resumed_at   timestamptz NULL,
            updated_at   timestamptz NOT NULL DEFAULT now()
        );

        -- Finished fan-out jobs are deleted on completion; clear rows written before that behavior.
        DELETE FROM wallaby.fanout_queue WHERE status = 'Completed';
        """;

    public static readonly IReadOnlyList<(int Version, string Ddl)> Steps = [(1, Baseline)];
}
