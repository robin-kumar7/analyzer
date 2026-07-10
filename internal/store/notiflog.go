// notiflog.go — notification deduplication via the notification_log table.
//
// Before forwarding an Issue to the notifier Kafka topic the analyzer
// checks whether the same bugFingerprint was sent within the configured
// cooldown window. If it was, the Issue is suppressed (logged at INFO so
// operators can see the suppression without noise). On a fresh or
// expired fingerprint the analyzer records the send and publishes.
//
// The cooldown is intentionally per-fingerprint (not per-tenant) so a
// bug that is firing across hundreds of ophids doesn't flood Teams — one
// notification per unique bug per window is enough.
package store

import (
	"context"
	"fmt"
	"time"
)

// NotifLogger is the minimal interface the Kafka consumer needs for
// notification deduplication. *Postgres implements it.
type NotifLogger interface {
	// ShouldNotify returns true when the fingerprint either hasn't been
	// notified before or its cooldown window has expired. On true the
	// caller MUST call MarkNotified immediately after a successful send.
	ShouldNotify(ctx context.Context, fingerprint string, cooldown time.Duration) (bool, error)
	// MarkNotified upserts the notification_log row for fingerprint.
	MarkNotified(ctx context.Context, fingerprint, title, serviceName string) error
}

// ShouldNotify checks whether the fingerprint is eligible for notification.
// Returns true (notify) when:
//   - no row exists yet for this fingerprint, OR
//   - the last_notified timestamp is older than cooldown.
//
// Returns false (suppress) when the fingerprint was notified within the
// cooldown window.
func (p *Postgres) ShouldNotify(ctx context.Context, fingerprint string, cooldown time.Duration) (bool, error) {
	if fingerprint == "" {
		return true, nil // can't deduplicate without a fingerprint; let it through
	}
	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	const sql = `
		SELECT last_notified
		  FROM notification_log
		 WHERE fingerprint = $1
		 LIMIT 1`

	var last time.Time
	err := p.pool.QueryRow(q, sql, fingerprint).Scan(&last)
	if err != nil {
		if isNoRows(err) {
			return true, nil // never notified
		}
		return true, fmt.Errorf("store: check notification_log: %w", err)
	}
	if time.Since(last) >= cooldown {
		return true, nil // cooldown expired
	}
	return false, nil // still in cooldown
}

// MarkNotified upserts the notification_log row so the next call to
// ShouldNotify for the same fingerprint returns false until the cooldown
// elapses again.
func (p *Postgres) MarkNotified(ctx context.Context, fingerprint, title, serviceName string) error {
	if fingerprint == "" {
		return nil
	}
	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	const sql = `
		INSERT INTO notification_log (fingerprint, last_notified, title, service_name)
		VALUES ($1, now(), $2, $3)
		ON CONFLICT (fingerprint) DO UPDATE
		   SET last_notified = now(),
		       title         = EXCLUDED.title,
		       service_name  = EXCLUDED.service_name`

	if _, err := p.pool.Exec(q, sql, fingerprint, title, serviceName); err != nil {
		return fmt.Errorf("store: mark notification_log: %w", err)
	}
	return nil
}
