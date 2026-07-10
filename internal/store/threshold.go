// Package store — per-service notification threshold lookup.
//
// The analyzer consumer calls LookupNotifyThreshold before deciding whether
// to forward a finished Issue to the notifier topic. Thresholds are read
// from service_mappings.notify_min_severity / notify_min_confidence and
// cached in-process for ThresholdCacheTTL to avoid a DB round-trip on
// every analyzed record.
//
// Thread-safe: a sync.Map holds the per-service cache entries; a simple
// time.Time expiry guards staleness without a background goroutine.
package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ThresholdCacheTTL is how long a resolved threshold is kept before the
// next record triggers a fresh DB lookup. Intentionally short so that
// a UI change propagates within a minute.
const ThresholdCacheTTL = 60 * time.Second

// NotifyThreshold is the per-service notification gate returned by
// LookupNotifyThreshold.
type NotifyThreshold struct {
	// MinSeverityRank is the minimum rank an Issue.Severity must have.
	// Rank mapping: low=1, medium=2, high=3, critical=4. 0 disables the check.
	MinSeverityRank int
	// MinConfidence is the minimum Issue.Confidence (0–1). 0 disables the check.
	MinConfidence float64
}

// ThresholdProvider is the minimal interface the Kafka consumer needs.
// Using an interface keeps the consumer testable without a real DB.
type ThresholdProvider interface {
	LookupNotifyThreshold(ctx context.Context, serviceName string) (NotifyThreshold, error)
}

// RepoMetaProvider looks up the repo URL + default branch for a
// service so downstream consumers (notifier) can build clickable
// GitHub source links from Issue.File / Fix.File.
type RepoMetaProvider interface {
	LookupRepoMeta(ctx context.Context, serviceName, resolvedRepo string) (RepoMeta, error)
}

// thresholdEntry is a cached lookup result.
type thresholdEntry struct {
	threshold NotifyThreshold
	expiresAt time.Time
}

// thresholdCache is the in-process TTL cache held by *Postgres.
type thresholdCache struct {
	mu      sync.Mutex
	entries map[string]thresholdEntry
}

func newThresholdCache() *thresholdCache {
	return &thresholdCache{entries: map[string]thresholdEntry{}}
}

// LookupNotifyThreshold returns the per-service threshold from
// service_mappings. Results are cached for ThresholdCacheTTL.
// On any lookup failure the caller receives a safe default (high/0.7)
// so the consumer degrades gracefully rather than blocking.
func (p *Postgres) LookupNotifyThreshold(ctx context.Context, serviceName string) (NotifyThreshold, error) {
	if p.thresholds == nil {
		p.thresholds = newThresholdCache()
	}

	p.thresholds.mu.Lock()
	if e, ok := p.thresholds.entries[serviceName]; ok && time.Now().Before(e.expiresAt) {
		p.thresholds.mu.Unlock()
		return e.threshold, nil
	}
	p.thresholds.mu.Unlock()

	// Cache miss or expired — query the DB.
	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	var minSev string
	var minConf float64
	err := p.pool.QueryRow(q,
		`SELECT notify_min_severity, notify_min_confidence
		   FROM service_mappings
		  WHERE service_name = $1
		  LIMIT 1`,
		serviceName,
	).Scan(&minSev, &minConf)
	if err != nil {
		// Service not in service_mappings or DB error: return safe default.
		return defaultThreshold(), fmt.Errorf("store: lookup notify threshold for %q: %w", serviceName, err)
	}

	t := NotifyThreshold{
		MinSeverityRank: SeverityRank(minSev),
		MinConfidence:   minConf,
	}

	p.thresholds.mu.Lock()
	p.thresholds.entries[serviceName] = thresholdEntry{
		threshold: t,
		expiresAt: time.Now().Add(ThresholdCacheTTL),
	}
	p.thresholds.mu.Unlock()

	return t, nil
}

// defaultThreshold is the safe fallback when no DB row exists.
// 'high' (rank 3) with 0.7 confidence is a conservative gate that
// avoids notification spam while ensuring genuinely severe issues
// always reach the team.
func defaultThreshold() NotifyThreshold {
	return NotifyThreshold{MinSeverityRank: 3, MinConfidence: 0.7}
}

// SeverityRank maps the severity string to an ordinal for comparison.
// Exported so the consumer and tests can reuse it without importing
// the kafkaio package.
func SeverityRank(s string) int {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "low":
		return 1
	case "medium":
		return 2
	case "high":
		return 3
	case "critical":
		return 4
	default:
		return 0
	}
}
