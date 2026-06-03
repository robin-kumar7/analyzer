// Package loki is intentionally empty.
//
// Loki / Grafana ingestion has been moved out of the analyzer into a
// separate, dedicated log-shipper service that POSTs entries to the
// analyzer's HTTP API (`POST /analyze`). This file is kept as a
// placeholder so the package directory remains in the tree.
package loki
