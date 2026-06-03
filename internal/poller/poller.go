// Package poller is intentionally empty.
//
// The in-process Loki poller has been removed from the analyzer.
// Log ingestion is now the responsibility of a separate service that
// consumes from Grafana/Loki and calls the analyzer's HTTP API. This
// file is kept as a placeholder so the package directory remains in
// the tree.
package poller
