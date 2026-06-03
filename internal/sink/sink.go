// Package sink is intentionally empty.
//
// Issue sinks (redis / slog / http) were specific to the in-process
// Loki poller, which has been removed. The HTTP API returns the
// analyzed Issue inline to the caller, so no sink layer is needed.
// Kept as a placeholder so the package directory remains in the tree.
package sink
