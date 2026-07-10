// Service-mapping read methods used by the analyzer Kafka consumer
// to translate the service identity stamped by log-shipper into the
// Weaviate repo that the retriever should scope to.
//
// Writes happen exclusively from analytics-api (which owns the HTTP
// surface). The analyzer is a read-only consumer of this table and
// caches lookups in Redis to keep the per-record overhead off the
// hot path.
package store

import (
	"context"
	"fmt"
	"time"
)

// ServiceMapping is the projection of one service_mappings row that
// the analyzer needs. Secret fields (git_token) are intentionally
// excluded — the analyzer never reads them.
type ServiceMapping struct {
	ServiceName  string
	Container    string
	WeaviateRepo string
	Enabled      bool
}

// LookupServiceMapping returns the mapping for the given service_name.
// Returns (zero, false, nil) when no row matches (caller decides how to
// fall back). Returns an error only on genuine DB failures.
func (p *Postgres) LookupServiceMapping(ctx context.Context, serviceName string) (ServiceMapping, bool, error) {
	if p == nil || p.pool == nil || serviceName == "" {
		return ServiceMapping{}, false, nil
	}

	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	const sql = `
		SELECT service_name, container, weaviate_repo, enabled
		  FROM service_mappings
		 WHERE service_name = $1`

	var m ServiceMapping
	err := p.pool.QueryRow(q, sql, serviceName).Scan(
		&m.ServiceName, &m.Container, &m.WeaviateRepo, &m.Enabled,
	)
	if err != nil {
		// pgx returns pgx.ErrNoRows on no match. Treat as not-found
		// rather than an error so the caller can fall back cleanly.
		if isNoRows(err) {
			return ServiceMapping{}, false, nil
		}
		return ServiceMapping{}, false, fmt.Errorf("postgres: lookup service_mapping %q: %w", serviceName, err)
	}
	return m, true, nil
}

// LookupServiceMappingByContainer is the same as LookupServiceMapping
// but indexed on the Loki container label. log-shipper used to emit
// only `containerId` (e.g. "cdc_grpc_in") instead of `service_name`,
// so the analyzer falls back to this when the service-name header
// is missing.
func (p *Postgres) LookupServiceMappingByContainer(ctx context.Context, container string) (ServiceMapping, bool, error) {
	if p == nil || p.pool == nil || container == "" {
		return ServiceMapping{}, false, nil
	}

	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()

	const sql = `
		SELECT service_name, container, weaviate_repo, enabled
		  FROM service_mappings
		 WHERE container = $1
		 LIMIT 1`

	var m ServiceMapping
	err := p.pool.QueryRow(q, sql, container).Scan(
		&m.ServiceName, &m.Container, &m.WeaviateRepo, &m.Enabled,
	)
	if err != nil {
		if isNoRows(err) {
			return ServiceMapping{}, false, nil
		}
		return ServiceMapping{}, false, fmt.Errorf("postgres: lookup service_mapping by container %q: %w", container, err)
	}
	return m, true, nil
}

// PingMappings runs a trivial query to confirm the table is reachable.
// Used by the cached resolver to surface schema/permission errors at
// boot instead of on the first Kafka batch.
func (p *Postgres) PingMappings(ctx context.Context) error {
	if p == nil || p.pool == nil {
		return fmt.Errorf("postgres: pool not initialized")
	}
	q, cancel := context.WithTimeout(ctx, p.queryTimeout)
	defer cancel()
	var n int
	if err := p.pool.QueryRow(q, `SELECT 1 FROM service_mappings LIMIT 1`).Scan(&n); err != nil {
		// Empty table is fine.
		if isNoRows(err) {
			return nil
		}
		return fmt.Errorf("postgres: ping service_mappings: %w", err)
	}
	return nil
}

// queryTimeoutFor returns the store's per-query timeout. Exposed so
// resolvers built on top of *Postgres can use the same deadline.
func (p *Postgres) QueryTimeout() time.Duration { return p.queryTimeout }
