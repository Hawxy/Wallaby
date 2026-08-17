using Wallaby.Client.Internal;

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
/// <item>Append only, with the next version number; never edit a shipped step.</item>
/// <item>Steps must be idempotent (<c>IF NOT EXISTS</c> / <c>ADD COLUMN IF NOT EXISTS</c>): a step
/// interrupted before its version stamp re-applies cleanly.</item>
/// <item>New columns must be nullable or carry <c>NOT NULL DEFAULT ...</c>; all host and client SQL
/// uses explicit column lists, so older writers keep working during rolling upgrades.</item>
/// <item>Never rename columns: the remote client tolerates missing tables (42P01) but not missing
/// columns, and the host's binary COPY into <c>stream_buffer</c> is position-sensitive.</item>
/// <item><c>wallaby.stream_buffer</c> is UNLOGGED scratch space cleared at session start; it may be
/// dropped and recreated in a step.</item>
/// </list>
/// </remarks>
internal static class StateSchemaMigrations
{
    /// <summary>
    /// The schema version this build requires; the highest version in <see cref="Steps"/>. Declared on
    /// the shared contract so the remote client's schema gate and this migration list agree; a new
    /// step bumps <see cref="ControlContract.SchemaVersion"/>.
    /// </summary>
    public const int CurrentVersion = ControlContract.SchemaVersion;

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
        -- ControlContract). Created only by the host; the remote client never performs DDL.
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

    /// <summary>Per-job retry state for the fan-out queue, so a failing job backs off on its own schedule.</summary>
    private const string FanoutRetryState = """
        ALTER TABLE wallaby.fanout_queue ADD COLUMN IF NOT EXISTS attempts int NOT NULL DEFAULT 0;
        ALTER TABLE wallaby.fanout_queue ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();
        -- Nullable: "no error" is the absence of a value, which older writers also leave null.
        ALTER TABLE wallaby.fanout_queue ADD COLUMN IF NOT EXISTS last_error text;

        -- Serves the worker's due-job scan: ... AND next_attempt_at <= now() ORDER BY next_attempt_at, requested_at.
        CREATE INDEX IF NOT EXISTS fanout_queue_next_attempt_idx
            ON wallaby.fanout_queue (next_attempt_at, requested_at)
            WHERE status IN ('Requested', 'InProgress');

        DROP INDEX IF EXISTS wallaby.fanout_queue_due_idx;
        """;

    private const string ControlAssertionHeartbeat = """
        ALTER TABLE wallaby.control ADD COLUMN IF NOT EXISTS configuration_asserted_at timestamptz;
        """;

    /// <summary>
    /// Set by a resume that asks for purging sink destinations; consumed by the slot-gap repair that
    /// serves the resume, or discarded when the next leader session finds no gap to repair.
    /// </summary>
    private const string ControlResumePurgeFlag = """
        ALTER TABLE wallaby.control ADD COLUMN IF NOT EXISTS purge_on_resume boolean NOT NULL DEFAULT false;
        """;

    /// <summary>
    /// Whether Wallaby owns the slot's publication (created it and can recreate it from configuration),
    /// authorizing suspension finalize to drop it alongside the slot. Defaults false so only a
    /// current-version provisioner stamp (never the migration itself) marks a publication droppable
    /// (an unmanaged <c>ManagePublicationTables=false</c> publication must never be dropped).
    /// </summary>
    private const string RegistryPublicationOwnership = """
        ALTER TABLE wallaby.slot_registry ADD COLUMN IF NOT EXISTS publication_managed boolean NOT NULL DEFAULT false;
        """;

    /// <summary>
    /// Set while managed publications are temporarily widened to whole-table membership so schema
    /// migrations blocked by publication column lists (ALTER COLUMN TYPE) can run without a suspension;
    /// cleared by a restore, applied by the next leader term's reconcile.
    /// </summary>
    private const string ControlPublicationWidening = """
        ALTER TABLE wallaby.control ADD COLUMN IF NOT EXISTS publications_widened boolean NOT NULL DEFAULT false;
        ALTER TABLE wallaby.control ADD COLUMN IF NOT EXISTS widened_at timestamptz NULL;
        ALTER TABLE wallaby.control ADD COLUMN IF NOT EXISTS widened_by text NULL;
        """;

    /// <summary>
    /// Per-table retry state for backfills, mirroring the fan-out queue's: a failing table backs off on
    /// its own schedule instead of faulting the leader session.
    /// </summary>
    private const string BackfillRetryState = """
        ALTER TABLE wallaby.backfill_state ADD COLUMN IF NOT EXISTS attempts int NOT NULL DEFAULT 0;
        ALTER TABLE wallaby.backfill_state ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();
        -- Nullable: "no error" is the absence of a value, which older writers also leave null.
        ALTER TABLE wallaby.backfill_state ADD COLUMN IF NOT EXISTS last_error text;
        """;

    /// <summary>
    /// Folds <c>wallaby.checkpoint</c> into <c>wallaby.slot_registry</c>: both tables were keyed by
    /// slot name and held LSN facts about the same slot, read together only by slot-gap repair. The
    /// provisioner registers every slot before its first checkpoint write, so a checkpoint row without
    /// a registry row cannot occur outside manual tampering; such orphans are dropped with the table.
    /// </summary>
    private const string CheckpointIntoRegistry = """
        ALTER TABLE wallaby.slot_registry ADD COLUMN IF NOT EXISTS confirmed_lsn pg_lsn NULL;
        ALTER TABLE wallaby.slot_registry ADD COLUMN IF NOT EXISTS checkpointed_at timestamptz NULL;

        DO $$
        BEGIN
            IF to_regclass('wallaby.checkpoint') IS NOT NULL THEN
                UPDATE wallaby.slot_registry r
                SET confirmed_lsn = c.confirmed_lsn, checkpointed_at = c.updated_at
                FROM wallaby.checkpoint c
                WHERE c.slot_name = r.slot_name;
                DROP TABLE wallaby.checkpoint;
            END IF;
        END $$;
        """;

    /// <summary>
    /// The W3C traceparent of the trigger that enqueued (or last re-armed) a fan-out job, so the scoped
    /// backfill's root span can link back to the triggering transaction's trace. Null when tracing was off.
    /// </summary>
    private const string FanoutTraceparent = """
        ALTER TABLE wallaby.fanout_queue ADD COLUMN IF NOT EXISTS traceparent text;
        """;

    public static readonly IReadOnlyList<(int Version, string Ddl)> Steps =
        [(1, Baseline), (2, FanoutRetryState), (3, ControlAssertionHeartbeat), (4, ControlResumePurgeFlag),
         (5, RegistryPublicationOwnership), (6, ControlPublicationWidening), (7, BackfillRetryState),
         (8, CheckpointIntoRegistry), (9, FanoutTraceparent)];
}
