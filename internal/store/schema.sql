-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  This file is a verbatim copy of analyzer/internal/store/schema.sql. ║
-- ║  Embedded into analytics-api so the API can self-bootstrap when      ║
-- ║  it boots before the analyzer. Both copies are applied idempotently  ║
-- ║  (IF NOT EXISTS / CREATE OR REPLACE) so whichever service wins the   ║
-- ║  boot race creates the schema; the other's apply is a no-op.        ║
-- ║                                                                      ║
-- ║  DO NOT EDIT this copy directly. Edit analyzer/internal/store/       ║
-- ║  schema.sql and copy it here:                                        ║
-- ║                                                                      ║
-- ║    cp analyzer/internal/store/schema.sql \                           ║
-- ║       analytics-api/internal/store/schema.sql                        ║
-- ╚══════════════════════════════════════════════════════════════════════╝

-- Analyzer Issue store — single table holds every Issue produced by
-- the analyzer pipeline (both Kafka and HTTP paths).
--
-- Indexes are tuned for the analytics-api query patterns:
--   GET /analytics/by-account     → (account_id, created_at DESC)
--   GET /analytics/by-service     → (service_name, created_at DESC)
--   GET /analytics/top-issues     → (fingerprint, created_at DESC)
--   GET /analytics/persistent     → (tenant_fingerprint, created_at DESC)
--   GET /analytics/trends         → (created_at) BRIN for time-bucketed scans
--
-- The full Issue and original log record are kept verbatim in JSONB
-- so the dashboard can render anything the Issue schema gains later
-- without a migration.

CREATE TABLE IF NOT EXISTS issues (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at          TIMESTAMPTZ  NOT NULL    DEFAULT now(),

    -- Provenance.
    source              TEXT         NOT NULL,                  -- 'kafka' | 'http'

    -- Tenancy (nullable; HTTP path has none).
    account_id          TEXT,
    flow_id             TEXT,
    ophid               TEXT,
    service_name        TEXT,

    -- Bug signature.
    resolved_repo       TEXT,
    file                TEXT,
    line                INTEGER,
    category            TEXT,
    severity            TEXT,
    title               TEXT,

    -- Quality.
    confidence          DOUBLE PRECISION,
    grounding_ok        BOOLEAN,

    -- Identity (sha256 hex; see fingerprint.go).
    fingerprint         TEXT         NOT NULL,                  -- bug identity (cross-tenant)
    tenant_fingerprint  TEXT         NOT NULL,                  -- bug × tenant identity

    -- Full payloads (lossless audit + future-proof).
    issue_json          JSONB        NOT NULL,
    log_json            JSONB
);

CREATE INDEX IF NOT EXISTS idx_issues_created_at
    ON issues (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_issues_account_created
    ON issues (account_id, created_at DESC)
    WHERE account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_issues_service_created
    ON issues (service_name, created_at DESC)
    WHERE service_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_issues_fingerprint_created
    ON issues (fingerprint, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_issues_tenant_fp_created
    ON issues (tenant_fingerprint, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_issues_severity_created
    ON issues (severity, created_at DESC)
    WHERE severity IS NOT NULL;

-- Status column for the analytics-ui issue list / detail. v1 has no
-- mutation path (everything stays 'open'), but the column is present
-- so the schema stays honest as workflow features land. Added via
-- ADD COLUMN IF NOT EXISTS so reapplying the schema against an
-- existing deployment is a no-op.
ALTER TABLE issues
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'open';

CREATE INDEX IF NOT EXISTS idx_issues_status_created
    ON issues (status, created_at DESC);

-- ---------------------------------------------------------------------------
-- service_mappings
--
-- Single source of truth that ties the three sides of the platform together:
--
--   log-shipper.service.name        →  Kafka header `service-name`
--   repo-indexer repo name (Weaviate)     →  RepoChunk.repo
--   container / Loki containerId    →  LogQL stream selector
--
-- The analyzer reads this table (with a Redis cache) to translate the
-- service identity that arrives on Kafka into the Weaviate repo that
-- the retriever should scope to. The log-shipper reads this table on
-- a short interval (polling) to drive its supervisor — no JSON file
-- and no restart required. The analytics-ui exposes full CRUD via
-- analytics-api.
--
-- Schema is owned by the analyzer (sole writer of issues; co-owner
-- of service_mappings with analytics-api which has write endpoints
-- for this table only).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS service_mappings (
    id                       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at               TIMESTAMPTZ  NOT NULL    DEFAULT now(),
    updated_at               TIMESTAMPTZ  NOT NULL    DEFAULT now(),

    -- Identity surfaced to log-shipper + analyzer (Kafka header
    -- `service-name`, Loki stream label `service_name`). Unique.
    service_name             TEXT         NOT NULL    UNIQUE,

    -- Loki container label. Drives the default LogQL stream
    -- selector when logql_override is NULL.
    container                TEXT         NOT NULL,

    -- Weaviate repo (RepoChunk.repo) that the analyzer should scope
    -- retrieval to for logs from this service. e.g. "cdc.grpc-in".
    weaviate_repo            TEXT         NOT NULL,

    -- Kafka topic the log-shipper publishes to. Defaults to the
    -- platform-wide "service-logs" topic.
    topic                    TEXT         NOT NULL    DEFAULT 'service-logs',

    -- log-shipper per-service overrides (all optional).
    logql_override           TEXT,
    poll_interval            TEXT,           -- Go duration ("30s", "1m"). NULL = use DEFAULT_POLL_INTERVAL.
    topic_partitions         INTEGER,
    topic_replication_factor INTEGER,

    -- repo-indexer hints (for the future "sync from UI" workflow; not
    -- consumed by repo-indexer today). Repo URL + branch are public; the
    -- token is a secret and MUST NEVER be returned in GET responses.
    repo_url                 TEXT,
    default_branch           TEXT,
    git_token                TEXT,

    -- Soft-disable without deleting history.
    enabled                  BOOLEAN      NOT NULL    DEFAULT true,

    -- Per-service notification thresholds (editable from the UI).
    notify_min_severity      TEXT         NOT NULL    DEFAULT 'high',
    notify_min_confidence    DOUBLE PRECISION         NOT NULL    DEFAULT 0.7,

    -- Local filesystem path on the ai-executor host (worktree mode).
    local_repo_path          TEXT
);

CREATE INDEX IF NOT EXISTS idx_service_mappings_enabled
    ON service_mappings (enabled, service_name);

-- Guards for existing deployments.
ALTER TABLE service_mappings ADD COLUMN IF NOT EXISTS notify_min_severity   TEXT             NOT NULL DEFAULT 'high';
ALTER TABLE service_mappings ADD COLUMN IF NOT EXISTS notify_min_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.7;
ALTER TABLE service_mappings ADD COLUMN IF NOT EXISTS local_repo_path       TEXT;

-- Keep updated_at honest. Used by the log-shipper poller to detect
-- changes since its last fetch.
CREATE OR REPLACE FUNCTION service_mappings_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS service_mappings_set_updated_at ON service_mappings;
CREATE TRIGGER service_mappings_set_updated_at
    BEFORE UPDATE ON service_mappings
    FOR EACH ROW
    EXECUTE FUNCTION service_mappings_touch_updated_at();

-- ---------------------------------------------------------------------------
-- integration_tokens
--
-- Stores write-only integration credentials (GitHub PAT, Jira API token).
-- One row per provider (key). The token column is NEVER returned by GET
-- endpoints — callers receive has_token (bool) only, mirroring the pattern
-- used by service_mappings.git_token.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration_tokens (
    key        TEXT        PRIMARY KEY,           -- 'github' | 'jira'
    token      TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- priority_customers
--
-- Manually-curated allowlist of "Priority Customer" accounts surfaced in
-- the analytics-ui "PC Accounts" tab. The account_id references the same
-- tenant identifier used on issues.account_id and onprem_host_info.account_id
-- (no FK — the issues table only stores an id, and empty/unknown accounts
-- are legal there).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS priority_customers (
    account_id   TEXT        PRIMARY KEY,
    display_name TEXT,
    note         TEXT,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- ai-executor: pr_url + pr_opened_at on issues
-- ---------------------------------------------------------------------------
ALTER TABLE issues
    ADD COLUMN IF NOT EXISTS pr_url       TEXT,
    ADD COLUMN IF NOT EXISTS pr_opened_at TIMESTAMPTZ;

-- ---------------------------------------------------------------------------
-- execution_jobs — persistent queue drained by the ai-executor service.
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE execution_status AS ENUM (
        'QUEUED',
        'CLAIMED',
        'RUNNING',
        'PUSHING',
        'SUCCEEDED',
        'FAILED',
        'CANCELLING',
        'CANCELLED'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS execution_jobs (
    id                  UUID              PRIMARY KEY DEFAULT gen_random_uuid(),
    issue_id            UUID              NOT NULL REFERENCES issues(id) ON DELETE CASCADE,

    status              execution_status  NOT NULL DEFAULT 'QUEUED',
    provider            TEXT              NOT NULL,
    model               TEXT,

    repo_url            TEXT              NOT NULL,
    base_branch         TEXT,
    head_branch         TEXT,

    prompt_hash         TEXT,
    instructions        TEXT,

    -- Lifecycle timestamps.
    created_at          TIMESTAMPTZ       NOT NULL DEFAULT now(),
    claimed_at          TIMESTAMPTZ,
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,

    -- Retry accounting.
    attempts            INT               NOT NULL DEFAULT 0,
    max_attempts        INT               NOT NULL DEFAULT 3,
    parent_job_id       UUID              REFERENCES execution_jobs(id) ON DELETE SET NULL,

    -- Result surface.
    pr_url              TEXT,
    pr_number           INT,
    commit_sha          TEXT,
    files_changed       INT,
    diff_bytes          INT,

    -- Live progress (updated during RUNNING; frozen at terminal).
    step                TEXT,
    step_detail         TEXT,

    -- Failure surface.
    error               TEXT,
    error_kind          TEXT,

    -- Worker identity.
    worker_id           TEXT,

    -- Resolved local filesystem path (worktree mode, no clone).
    workspace_path      TEXT,

    -- Reoccurrence tracking: how many times this bug fired again
    -- while this job was the most recent for its fingerprint. The
    -- audit trail lives in execution_job_reoccurrences (below).
    reoccurrence_count  INT               NOT NULL DEFAULT 0,
    last_reoccurred_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_execution_jobs_status_created
    ON execution_jobs (status, created_at)
    WHERE status IN ('QUEUED', 'CLAIMED', 'RUNNING', 'PUSHING', 'CANCELLING');

CREATE INDEX IF NOT EXISTS idx_execution_jobs_issue_created
    ON execution_jobs (issue_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_jobs_created
    ON execution_jobs (created_at DESC);

-- Guard for existing deployments.
ALTER TABLE execution_jobs ADD COLUMN IF NOT EXISTS reoccurrence_count INT NOT NULL DEFAULT 0;
ALTER TABLE execution_jobs ADD COLUMN IF NOT EXISTS last_reoccurred_at TIMESTAMPTZ;

-- ---------------------------------------------------------------------------
-- execution_job_reoccurrences — audit trail of "same bug fired again".
--
-- Written every time analytics-api receives an execute request whose
-- issue.fingerprint matches an existing job (QUEUED/RUNNING/SUCCEEDED).
-- Instead of enqueuing a duplicate job we record the reoccurrence here
-- and bump execution_jobs.reoccurrence_count. The original issue_id is
-- stored so operators can trace which log occurrence re-triggered.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS execution_job_reoccurrences (
    id               BIGSERIAL   PRIMARY KEY,
    job_id           UUID        NOT NULL REFERENCES execution_jobs(id) ON DELETE CASCADE,
    issue_id         UUID        NOT NULL,
    fingerprint      TEXT        NOT NULL,
    occurred_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    note             TEXT
);

CREATE INDEX IF NOT EXISTS idx_execution_job_reoccurrences_job_time
    ON execution_job_reoccurrences (job_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_job_reoccurrences_fingerprint
    ON execution_job_reoccurrences (fingerprint, occurred_at DESC);

-- ---------------------------------------------------------------------------
-- Global AI / Local AI execution modes (docs/12-ai-executor-execution-modes.md)
--
-- 'DISPATCHED' sits between QUEUED and RUNNING: a local-mode row has been
-- pushed to a local executor's tunnel URL and 202-accepted, but no
-- heartbeat has arrived yet. Global-mode jobs never enter this state.
-- ---------------------------------------------------------------------------
ALTER TYPE execution_status ADD VALUE IF NOT EXISTS 'DISPATCHED';

-- SCHEMA_SPLIT_COMMIT --
-- PostgreSQL requires ALTER TYPE ... ADD VALUE to be committed before the
-- new value can be referenced in the same session (SQLSTATE 55P04).
-- The store.New function splits on this marker and executes each segment
-- in a separate Exec call so the ADD VALUE transaction is committed first.

ALTER TABLE execution_jobs
    ADD COLUMN IF NOT EXISTS execution_mode      TEXT NOT NULL DEFAULT 'global' CHECK (execution_mode IN ('global', 'local')),
    ADD COLUMN IF NOT EXISTS local_executor_url  TEXT,               -- tunnel URL used for this job (local mode only)
    ADD COLUMN IF NOT EXISTS callback_token      TEXT,               -- random secret the agent must present on heartbeat/report; NEVER returned by any GET
    ADD COLUMN IF NOT EXISTS dispatched_at       TIMESTAMPTZ,        -- when analytics-api POSTed to the tunnel URL
    ADD COLUMN IF NOT EXISTS last_heartbeat_at   TIMESTAMPTZ;        -- bumped on every /heartbeat call (local mode only)

CREATE INDEX IF NOT EXISTS idx_execution_jobs_local_heartbeat_sweep
    ON execution_jobs (execution_mode, status, last_heartbeat_at)
    WHERE execution_mode = 'local' AND status IN ('DISPATCHED', 'RUNNING', 'PUSHING');

-- ai_executor_settings — singleton row (the standard Postgres
-- singleton-row idiom: id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
-- so a second INSERT always violates the check).
CREATE TABLE IF NOT EXISTS ai_executor_settings (
    id                          SMALLINT     PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    mode                        TEXT         NOT NULL DEFAULT 'global' CHECK (mode IN ('global', 'local')),
    heartbeat_interval_seconds  INT          NOT NULL DEFAULT 10,
    heartbeat_timeout_seconds   INT          NOT NULL DEFAULT 30,
    updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

INSERT INTO ai_executor_settings (id) VALUES (1)
    ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- notification_log (mirrors analyzer/internal/store/schema.sql)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS notification_log (
    fingerprint     TEXT        PRIMARY KEY,
    last_notified   TIMESTAMPTZ NOT NULL DEFAULT now(),
    title           TEXT,
    service_name    TEXT
);

CREATE INDEX IF NOT EXISTS idx_notification_log_last
    ON notification_log (last_notified DESC);
