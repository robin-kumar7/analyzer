// Package mapper resolves a log-shipper service identity (the value
// stamped on Kafka header `service-name` or the Loki stream label
// `service_name` / `containerId`) to the Weaviate repo that the
// analyzer's retriever should scope to.
//
// The truth source is the `service_mappings` table owned by the
// analyzer schema and CRUD-managed via analytics-api. Lookups go
// through a Redis cache with a short TTL so dashboard edits take
// effect without a restart but per-record overhead stays in the
// sub-millisecond range.
//
// All methods are safe for concurrent use. nil receivers are valid
// and return ("", false, nil) — the consumer treats that as "no
// mapping, fall back to the raw service identity".
package mapper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/infoblox/vibecoder-analyzer/internal/store"
)

// PostgresLookup is the subset of *store.Postgres that the resolver
// needs. Declared here so tests can substitute a fake without
// pulling in pgx.
type PostgresLookup interface {
	LookupServiceMapping(ctx context.Context, serviceName string) (store.ServiceMapping, bool, error)
	LookupServiceMappingByContainer(ctx context.Context, container string) (store.ServiceMapping, bool, error)
}

// Sentinel cache value indicating "we asked Postgres and there is no
// row for this key". Stored under the same TTL as positive hits so
// every unmapped service doesn't pound the DB once per Kafka batch.
const negativeCacheSentinel = "\x00MISS\x00"

// Resolver translates service identities to Weaviate repos.
type Resolver struct {
	pg        PostgresLookup
	rdb       *redis.Client
	ttl       time.Duration
	negTTL    time.Duration
	keyPrefix string
}

// Options configures a Resolver.
type Options struct {
	// DB is the Postgres-backed read source. May be nil to disable
	// dynamic resolution entirely (the resolver then always returns
	// the input unchanged).
	DB PostgresLookup
	// Redis is the cache. May be nil — lookups then go straight to
	// Postgres on every call (acceptable for low-volume deployments).
	Redis *redis.Client
	// TTL is the lifetime of positive (hit) cache entries. <= 0 falls
	// back to 60s.
	TTL time.Duration
	// NegativeTTL is the lifetime of "no row" sentinel entries. <= 0
	// falls back to TTL / 2 (so misconfigured services recover faster
	// than the steady-state cache age).
	NegativeTTL time.Duration
	// KeyPrefix is prepended to every Redis key. Defaults to
	// "analyzer:svc-map:".
	KeyPrefix string
}

// New builds a Resolver. The returned value is always non-nil; pass
// nil Options.DB to get a pass-through resolver.
func New(opts Options) *Resolver {
	if opts.TTL <= 0 {
		opts.TTL = 60 * time.Second
	}
	if opts.NegativeTTL <= 0 {
		opts.NegativeTTL = opts.TTL / 2
		if opts.NegativeTTL <= 0 {
			opts.NegativeTTL = 30 * time.Second
		}
	}
	if opts.KeyPrefix == "" {
		opts.KeyPrefix = "analyzer:svc-map:"
	}
	return &Resolver{
		pg:        opts.DB,
		rdb:       opts.Redis,
		ttl:       opts.TTL,
		negTTL:    opts.NegativeTTL,
		keyPrefix: opts.KeyPrefix,
	}
}

// Resolve translates a service identity to the Weaviate repo it
// should be retrieved against. Returns ("", false) when no mapping
// exists or the mapping is disabled — callers should fall back to
// the raw identity (preserving today's heuristic behaviour).
//
// Lookup order:
//  1. Redis cache (positive or negative sentinel)
//  2. Postgres `service_mappings.service_name = $1`
//  3. Postgres `service_mappings.container = $1` (fallback for old
//     log-shipper records that only carry the container label)
func (r *Resolver) Resolve(ctx context.Context, identity string) (repo string, ok bool) {
	if r == nil || identity == "" {
		return "", false
	}

	// 1. Cache.
	if cached, hit := r.cacheGet(ctx, identity); hit {
		if cached == "" {
			return "", false
		}
		return cached, true
	}

	// 2/3. DB.
	repo, ok = r.lookupDB(ctx, identity)

	// Populate cache (positive or negative).
	r.cacheSet(ctx, identity, repo, ok)
	return repo, ok
}

func (r *Resolver) lookupDB(ctx context.Context, identity string) (string, bool) {
	if r.pg == nil {
		return "", false
	}

	// Primary: service_name match.
	m, found, err := r.pg.LookupServiceMapping(ctx, identity)
	if err != nil {
		slog.Warn("service-mapping lookup failed", "identity", identity, "error", err)
		return "", false
	}
	if found && m.Enabled && m.WeaviateRepo != "" {
		return m.WeaviateRepo, true
	}

	// Fallback: container match (log-shipper used to emit only
	// containerId; analyzer/kafkaio/consumer.go falls back to it).
	m, found, err = r.pg.LookupServiceMappingByContainer(ctx, identity)
	if err != nil {
		slog.Warn("service-mapping container lookup failed", "identity", identity, "error", err)
		return "", false
	}
	if found && m.Enabled && m.WeaviateRepo != "" {
		return m.WeaviateRepo, true
	}

	return "", false
}

func (r *Resolver) cacheGet(ctx context.Context, identity string) (string, bool) {
	if r.rdb == nil {
		return "", false
	}
	val, err := r.rdb.Get(ctx, r.keyPrefix+identity).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			slog.Debug("service-mapping cache get failed", "identity", identity, "error", err)
		}
		return "", false
	}
	if val == negativeCacheSentinel {
		return "", true // miss was cached — caller treats empty repo as "no mapping"
	}
	return val, true
}

func (r *Resolver) cacheSet(ctx context.Context, identity, repo string, ok bool) {
	if r.rdb == nil {
		return
	}
	val := repo
	ttl := r.ttl
	if !ok {
		val = negativeCacheSentinel
		ttl = r.negTTL
	}
	if err := r.rdb.Set(ctx, r.keyPrefix+identity, val, ttl).Err(); err != nil {
		slog.Debug("service-mapping cache set failed", "identity", identity, "error", err)
	}
}

// Invalidate drops the cache entry for the given identity. Called
// when analytics-api detects a mutation it knows the analyzer is
// caching; not on the hot path.
func (r *Resolver) Invalidate(ctx context.Context, identity string) error {
	if r == nil || r.rdb == nil || identity == "" {
		return nil
	}
	if err := r.rdb.Del(ctx, r.keyPrefix+identity).Err(); err != nil {
		return fmt.Errorf("mapper: invalidate %q: %w", identity, err)
	}
	return nil
}
