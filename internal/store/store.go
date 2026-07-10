// Package store persists analyzed Issues to a backing datastore
// (currently PostgreSQL) for offline analytics and dashboards.
//
// The store is best-effort: errors are logged at warn level and never
// fail the analysis pipeline. A nil Store (or the no-op fallback) is
// the disabled state used when no analytics database is configured.
//
// The analyzer is the only writer. The companion analytics-api service
// reads from the same table.
package store

import (
	"context"
	"log/slog"

	"github.com/infoblox/vibecoder-analyzer/internal/issue"
)

// Source identifies the pipeline path that produced an Issue.
type Source string

const (
	// SourceKafka is an Issue produced by the Kafka consumer path
	// (records ingested from the log-shipper service-logs topic).
	SourceKafka Source = "kafka"
	// SourceHTTP is an Issue produced by the POST /analyze HTTP path.
	// HTTP rows carry no tenant context (account / flow / ophid are
	// NULL); they aggregate under service_name only.
	SourceHTTP Source = "http"
)

// Store persists analyzed Issues.
//
// Implementations must be safe for concurrent use. They MUST NOT block
// the caller for longer than the supplied context allows.
type Store interface {
	// Save persists a single Issue together with its log context. logCtx
	// is the same map produced by the Kafka consumer's buildLogCtx (and
	// is nil for HTTP-path callers). Implementations should treat any
	// missing field as NULL / unknown and never reject a write because
	// the tenant tuple is empty.
	Save(ctx context.Context, iss *issue.Issue, logCtx map[string]any, source Source) error

	// Close releases any underlying resources (connection pools, etc.).
	// Safe to call multiple times.
	Close()
}

// NoOp returns a Store that drops every write. Used when no analytics
// database is configured so callers don't need nil-checks at every
// call site.
func NoOp() Store { return noOpStore{} }

type noOpStore struct{}

func (noOpStore) Save(context.Context, *issue.Issue, map[string]any, Source) error { return nil }
func (noOpStore) Close()                                                           {}

// SaveOrLog calls s.Save and logs (at warn) any error. It NEVER returns
// the error to the caller — persistence is best-effort.
//
// Use this from the Kafka consumer and HTTP handler so a transient DB
// outage does not fail the analysis pipeline.
func SaveOrLog(ctx context.Context, s Store, iss *issue.Issue, logCtx map[string]any, source Source) {
	if s == nil || iss == nil {
		return
	}
	// Skip the verbose log for the no-op store so we don't pretend
	// to be writing when no DB is configured.
	_, isNoOp := s.(noOpStore)
	if !isNoOp {
		slog.Info("store: saving issue",
			"source", string(source),
			"service_name", iss.ServiceName,
			"resolved_repo", iss.ResolvedRepo,
			"severity", iss.Severity,
			"confidence", iss.Confidence,
			"grounding_ok", iss.GroundingOK,
			"title", iss.Title)
	}
	if err := s.Save(ctx, iss, logCtx, source); err != nil {
		slog.Warn("analytics store save failed",
			"source", string(source),
			"service_name", iss.ServiceName,
			"resolved_repo", iss.ResolvedRepo,
			"error", err)
		return
	}
	if !isNoOp {
		slog.Info("store: issue saved",
			"source", string(source),
			"service_name", iss.ServiceName,
			"severity", iss.Severity)
	}
}
