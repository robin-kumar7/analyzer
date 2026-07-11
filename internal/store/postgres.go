// Postgres-backed implementation of store.Store.
//
// Uses jackc/pgx/v5 with a connection pool. Schema is applied at
// construction time via go:embed schema.sql (idempotent — relies on
// CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/infoblox/vibecoder-analyzer/internal/issue"
)

//go:embed schema.sql
var schemaSQL string

// PostgresOptions configures a Postgres-backed Store.
type PostgresOptions struct {
	// URL is a postgres:// DSN. Required.
	URL string
	// QueryTimeout caps every individual SQL call (Save, Prune, Close).
	QueryTimeout time.Duration
	// MaxConns caps the pool size. 0 = pgx default.
	MaxConns int
}

// Postgres is a *pgxpool.Pool-backed Store.
type Postgres struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
	thresholds   *thresholdCache // lazy-initialised; nil until first LookupNotifyThreshold call
}

// NewPostgres opens a connection pool, pings it, and applies the schema.
// Returns an error if any of those steps fails — callers are expected
// to fall back to store.NoOp() and continue running.
func NewPostgres(ctx context.Context, opts PostgresOptions) (*Postgres, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("postgres: URL is required")
	}
	if opts.QueryTimeout <= 0 {
		opts.QueryTimeout = 10 * time.Second
	}

	cfg, err := pgxpool.ParseConfig(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse url: %w", err)
	}
	if opts.MaxConns > 0 {
		cfg.MaxConns = int32(opts.MaxConns)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: open pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, opts.QueryTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	migrateCtx, cancel2 := context.WithTimeout(ctx, opts.QueryTimeout)
	defer cancel2()
	if err := applyPostgresSchema(migrateCtx, pool, schemaSQL); err != nil {
		pool.Close()
		return nil, err
	}

	return &Postgres{pool: pool, queryTimeout: opts.QueryTimeout}, nil
}

// applyPostgresSchema executes the embedded schema SQL, splitting at
// "-- SCHEMA_SPLIT_COMMIT --" so that ALTER TYPE ... ADD VALUE
// statements are committed before any DDL that references the new
// enum value (PostgreSQL SQLSTATE 55P04 guard).
func applyPostgresSchema(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	const splitMarker = "-- SCHEMA_SPLIT_COMMIT --"
	segments := strings.SplitN(sql, splitMarker, 2)
	for i, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		if _, err := pool.Exec(ctx, seg); err != nil {
			return fmt.Errorf("postgres: apply schema (segment %d): %w", i+1, err)
		}
	}
	return nil
}

// Save inserts a single Issue row. Errors propagate to the caller —
// use SaveOrLog (above) from pipeline code to make this non-fatal.
func (p *Postgres) Save(ctx context.Context, iss *issue.Issue, logCtx map[string]any, source Source) error {
	if iss == nil {
		return fmt.Errorf("postgres: nil issue")
	}

	accountID, flowID, ophID := tenancyFromLogCtx(logCtx)

	issueJSON, err := json.Marshal(iss)
	if err != nil {
		return fmt.Errorf("postgres: marshal issue: %w", err)
	}
	var logJSON []byte
	if len(logCtx) > 0 {
		logJSON, err = json.Marshal(logCtx)
		if err != nil {
			return fmt.Errorf("postgres: marshal log ctx: %w", err)
		}
	}

	fp := bugFingerprint(iss.ServiceName, iss.ResolvedRepo, iss.File, iss.Line, iss.Category)
	tfp := tenantFingerprint(accountID, flowID, ophID, iss.ServiceName, iss.File, iss.Line, iss.Category)

	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	const sql = `
		INSERT INTO issues (
			source,
			account_id, flow_id, ophid, service_name,
			resolved_repo, file, line, category, severity, title,
			confidence, grounding_ok,
			fingerprint, tenant_fingerprint,
			issue_json, log_json
		) VALUES (
			$1,
			$2, $3, $4, $5,
			$6, $7, $8, $9, $10, $11,
			$12, $13,
			$14, $15,
			$16, $17
		)
		RETURNING id`
	var id string
	err = p.pool.QueryRow(q, sql,
		string(source),
		nullable(accountID), nullable(flowID), nullable(ophID), nullable(iss.ServiceName),
		nullable(iss.ResolvedRepo), nullable(iss.File), nullableInt(iss.Line),
		nullable(iss.Category), nullable(iss.Severity), nullable(iss.Title),
		nullableFloat(iss.Confidence), iss.GroundingOK,
		fp, tfp,
		issueJSON, logJSON,
	).Scan(&id)
	if err != nil {
		return fmt.Errorf("postgres: insert issue: %w", err)
	}
	// Stash the ID on logCtx so the downstream publisher can inject
	// it into the Kafka envelope for the notifier's "View issue"
	// deep link. Safe on nil map: caller must check before use.
	if logCtx != nil {
		logCtx["issue_id"] = id
	}
	return nil
}

// RepoMeta is the subset of service_mappings needed to build deep
// links (repo URL + branch) for downstream consumers like the
// notifier's Teams card "Open source" action.
type RepoMeta struct {
	RepoURL       string
	DefaultBranch string
}

// LookupRepoMeta returns the repo URL + default branch for the
// service_mappings row matching serviceName, resolvedRepo, or
// container (in that order). Any of them may hit a row because a
// single row can be keyed on any of the three columns depending on
// how the service was registered. Returns an empty struct and nil
// error when no row matches — callers should treat that as "no
// deep link available", not an error.
func (p *Postgres) LookupRepoMeta(ctx context.Context, serviceName, resolvedRepo string) (RepoMeta, error) {
	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	const sql = `
		SELECT COALESCE(repo_url, ''), COALESCE(default_branch, '')
		  FROM service_mappings
		 WHERE service_name  = $1
		    OR weaviate_repo = $1
		    OR container     = $1
		    OR service_name  = $2
		    OR weaviate_repo = $2
		    OR container     = $2
		 LIMIT 1`

	var meta RepoMeta
	if err := p.pool.QueryRow(q, sql, serviceName, resolvedRepo).Scan(&meta.RepoURL, &meta.DefaultBranch); err != nil {
		if isNoRows(err) {
			return RepoMeta{}, nil
		}
		return RepoMeta{}, fmt.Errorf("postgres: lookup repo meta: %w", err)
	}
	return meta, nil
}

// Prune deletes rows older than retention. Returns the number of rows
// deleted. retention <= 0 is a no-op (returns 0, nil).
func (p *Postgres) Prune(ctx context.Context, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}
	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	const sql = `DELETE FROM issues WHERE created_at < now() - $1::interval`
	cmd, err := p.pool.Exec(q, sql, retention.String())
	if err != nil {
		return 0, fmt.Errorf("postgres: prune: %w", err)
	}
	return cmd.RowsAffected(), nil
}

// Close releases the connection pool. Safe to call multiple times.
func (p *Postgres) Close() {
	if p == nil || p.pool == nil {
		return
	}
	p.pool.Close()
	p.pool = nil
}

// tenancyFromLogCtx extracts the (account_id, flow_id, ophid) tuple
// from a log context map. All three are optional; missing values are
// returned as empty strings.
//
// Recognized aliases (stream first, then a regex fallback on the
// top-level `line` field for in-text identifiers):
//
//	account_id ← stream.account_id | customer_id | accountId
//	flow_id    ← stream.flow_id
//	              | line text: "flow ID [<id> ...]" (first id wins)
//	ophid      ← stream.ophid | oph_id | agentId | agent_id
//	              | line text: "ophid <hash>"
//
// The line-text fallback exists because cdc_grpc_in (and similar
// services) embed the structured ids in free-form log messages
// rather than as Promtail labels.
func tenancyFromLogCtx(logCtx map[string]any) (account, flow, oph string) {
	if len(logCtx) == 0 {
		return "", "", ""
	}
	stream, _ := logCtx["stream"].(map[string]any)
	if len(stream) > 0 {
		account = firstString(stream, "account_id", "customer_id", "accountId")
		flow = firstString(stream, "flow_id")
		oph = firstString(stream, "ophid", "oph_id", "agentId", "agent_id")
	}

	// Fall back to regex-extracting identifiers from the log line
	// text. Only fills empty slots so explicit stream labels win.
	if flow == "" || oph == "" {
		line, _ := logCtx["line"].(string)
		if line != "" {
			if oph == "" {
				oph = extractOphIDFromLine(line)
			}
			if flow == "" {
				flow = extractFlowIDFromLine(line)
			}
		}
	}
	return account, flow, oph
}

var (
	// ophidLineRe matches "ophid <hash>" with a hex-ish identifier of
	// reasonable length. Case-insensitive on the keyword; the value
	// itself is taken verbatim so we don't normalize away upstream
	// case (some services use uppercase hashes).
	ophidLineRe = regexp.MustCompile(`(?i)\bop[h_-]?id[\s:=]+([A-Za-z0-9_-]{8,128})\b`)

	// flowIDLineRe matches either:
	//   - bracketed list: "flow ID [123 456 789]" — capture inside
	//   - single id: "flow ID 123" / "flow_id=123" — capture value
	// In both cases we pull just the first numeric token.
	flowIDLineRe = regexp.MustCompile(`(?i)\bflow[\s_-]?id[\s:=]+(?:\[([\s\d]+)\]|(\d+))`)
)

func extractOphIDFromLine(line string) string {
	m := ophidLineRe.FindStringSubmatch(line)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

func extractFlowIDFromLine(line string) string {
	m := flowIDLineRe.FindStringSubmatch(line)
	if len(m) < 3 {
		return ""
	}
	// Bracketed list — take the first whitespace-separated token.
	if m[1] != "" {
		for _, tok := range strings.Fields(m[1]) {
			if tok != "" {
				return tok
			}
		}
		return ""
	}
	return m[2]
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// nullable returns nil for empty strings so they land as SQL NULL.
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullableInt returns nil for non-positive ints so unknown line
// numbers land as SQL NULL (matches the convention in the Issue
// schema where 0 means "unknown").
func nullableInt(n int) any {
	if n <= 0 {
		return nil
	}
	return n
}

// nullableFloat returns nil for NaN / negative confidence so they
// land as SQL NULL.
func nullableFloat(f float64) any {
	if f != f || f < 0 { // NaN check via f != f
		return nil
	}
	return f
}

// isNoRows is true when err is pgx's "no rows in result set" error.
// Lookup methods translate this to a (zero, false, nil) result so
// callers can branch on existence without depending on pgx types.
func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
