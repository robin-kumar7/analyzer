// Package store — CDC row helpers.
//
// The analyzer reads the full log context from the cdc_* table that the
// log-classifier wrote (FetchCDCRow), runs its pipeline, then writes the
// result back to the same row (UpdateCDCFinal). Both operations require a
// dynamic table name because every service gets its own cdc_<container>
// table.
//
// Security: table names come from the logs.enriched Kafka envelope which
// is produced by log-classifier. Before constructing any SQL string we
// reject names that don't carry the "cdc_" prefix and validate that the
// suffix is [a-z0-9_]+ only, matching the sanitization the classifier
// enforces in routing.TableName.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/infoblox/vibecoder-analyzer/internal/issue"
)

// reCDCSuffix allows only lowercase alphanumerics and underscores after the
// "cdc_" prefix. This mirrors log-classifier's routing.TableName sanitizer.
var reCDCSuffix = regexp.MustCompile(`^[a-z0-9_]+$`)

// ErrTableMissing is returned by FetchCDCRow when the target cdc_*
// table does not exist in Postgres. Callers should treat this as a
// benign "skip and commit the offset" outcome rather than a hard
// failure — it happens when Kafka still holds messages produced
// against a cdc_* table that has since been dropped (e.g. the old
// cdc_other fallback that was removed from the classifier).
var ErrTableMissing = errors.New("store: cdc table does not exist")

// validateCDCTable returns an error when name is not a safe cdc_* identifier.
func validateCDCTable(name string) error {
	if !strings.HasPrefix(name, "cdc_") {
		return fmt.Errorf("store: table %q does not have cdc_ prefix", name)
	}
	suffix := strings.TrimPrefix(name, "cdc_")
	if suffix == "" || !reCDCSuffix.MatchString(suffix) {
		return fmt.Errorf("store: table %q has invalid suffix", name)
	}
	return nil
}

// quoteIdent wraps name in double-quotes for use as a Postgres identifier.
// Only called after validateCDCTable has confirmed the name is safe.
func quoteIdentCDC(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// CDCRow is the subset of a cdc_* row that the analyzer needs to run its
// pipeline. It is returned by FetchCDCRow.
type CDCRow struct {
	ID            int64
	Raw           string // verbatim log line written by log-classifier
	WeaviateRepo  string // scoping key for Weaviate queries; empty when unset
	ServiceName   string
	Severity      string
	ParsedJSON    []byte // classifier parsed signals (JSONB), may be nil
	RetrievalJSON []byte // classifier Weaviate retrieval (JSONB), may be nil
}

// FetchCDCRow reads a single row from the named cdc_* table by primary key.
// Returns an error (wrapping pgx.ErrNoRows) when the row does not exist.
func (p *Postgres) FetchCDCRow(ctx context.Context, tableName string, rowID int64) (*CDCRow, error) {
	if err := validateCDCTable(tableName); err != nil {
		return nil, err
	}

	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	sql := fmt.Sprintf(
		`SELECT id, raw, weaviate_repo, service_name, severity, parsed, retrieval
		   FROM %s WHERE id = $1`,
		quoteIdentCDC(tableName),
	)

	row := p.pool.QueryRow(q, sql, rowID)
	var r CDCRow
	var weaviateRepo *string // nullable in Postgres; map NULL → ""
	err := row.Scan(
		&r.ID,
		&r.Raw,
		&weaviateRepo,
		&r.ServiceName,
		&r.Severity,
		&r.ParsedJSON,
		&r.RetrievalJSON,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("store: cdc row %d not found in %s: %w", rowID, tableName, pgx.ErrNoRows)
		}
		// PgError 42P01 = undefined_table. This is not a driver bug or a
		// transient failure — the table is gone and no retry will bring
		// it back. Return a sentinel so the caller can log at WARN and
		// commit the offset instead of ERROR-looping.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return nil, fmt.Errorf("%w: %s", ErrTableMissing, tableName)
		}
		return nil, fmt.Errorf("store: fetch cdc row: %w", err)
	}
	if weaviateRepo != nil {
		r.WeaviateRepo = *weaviateRepo
	}
	return &r, nil
}

// UpdateCDCFinal writes the analyzer's final verdict back into the cdc_* row.
// It sets final_status='analyzed' and stamps final_at. On a nil iss it marks
// the row as 'failed' so the UI can surface it without leaving rows stuck in
// 'pending' forever.
func (p *Postgres) UpdateCDCFinal(ctx context.Context, tableName string, rowID int64, iss *issue.Issue) error {
	if err := validateCDCTable(tableName); err != nil {
		return err
	}

	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	if iss == nil {
		sql := fmt.Sprintf(
			`UPDATE %s SET final_status='failed', final_at=$1 WHERE id=$2`,
			quoteIdentCDC(tableName),
		)
		_, err := p.pool.Exec(q, sql, time.Now().UTC(), rowID)
		if err != nil {
			return fmt.Errorf("store: update cdc final (failed): %w", err)
		}
		return nil
	}

	issJSON, err := json.Marshal(iss)
	if err != nil {
		return fmt.Errorf("store: marshal issue: %w", err)
	}

	sql := fmt.Sprintf(`
		UPDATE %s SET
			final_title        = $1,
			final_category     = $2,
			final_severity     = $3,
			final_summary      = $4,
			final_root_cause   = $5,
			final_file         = $6,
			final_line         = $7,
			final_confidence   = $8,
			final_grounding_ok = $9,
			final_issue_json   = $10,
			final_status       = 'analyzed',
			final_at           = $11
		WHERE id = $12`,
		quoteIdentCDC(tableName),
	)
	_, err = p.pool.Exec(q, sql,
		nullableStr(iss.Title),
		nullableStr(iss.Category),
		nullableStr(iss.Severity),
		nullableStr(iss.Problem),
		nullableStr(iss.RootCause),
		nullableStr(iss.File),
		nullableIntPtr(iss.Line),
		nullableFloatPtr(iss.Confidence),
		iss.GroundingOK,
		issJSON,
		time.Now().UTC(),
		rowID,
	)
	if err != nil {
		return fmt.Errorf("store: update cdc final: %w", err)
	}
	return nil
}

// CDCUpdater is the minimal interface the Kafka consumer needs.
// Using an interface keeps the consumer testable without a real DB.
type CDCUpdater interface {
	FetchCDCRow(ctx context.Context, tableName string, rowID int64) (*CDCRow, error)
	UpdateCDCFinal(ctx context.Context, tableName string, rowID int64, iss *issue.Issue) error
}

// ── helpers ───────────────────────────────────────────────────────────────

func nullableStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableIntPtr(n int) any {
	if n <= 0 {
		return nil
	}
	return n
}

func nullableFloatPtr(f float64) any {
	if f <= 0 {
		return nil
	}
	return f
}
