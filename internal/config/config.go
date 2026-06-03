// Package config loads analyzer configuration from environment variables.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config holds all analyzer settings. Loaded from environment (NFR-7).
type Config struct {
	Port            int
	OllamaURL       string
	OllamaModel     string
	OllamaTimeout   time.Duration
	EmbedModel      string
	EmbedTimeout    time.Duration
	WeaviateURL     string
	WeaviateClass   string
	WeaviateTimeout time.Duration
	DefaultTopK     int
	HybridAlpha     float64

	// Multi-repo resolution (FR-A5b).
	ResolveRepoMinScore  float64
	ResolveRepoAmbiguity float64

	// Grounding (B1).
	AnchorLen int
	AnchorMin int

	// HTTP server.
	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration
	HTTPIdleTimeout  time.Duration
	MaxBodyBytes     int64

	// Auth & rate limit.
	APIKey         string
	RateLimitRPS   float64
	RateLimitBurst int

	// Logging.
	LogLevel  string
	LogFormat string

	// Redis (summary cache only).
	RedisURL string

	// Documentation priming / service-summary cache (§7a).
	DocsPathPrefix        string
	SummaryEnabled        bool
	SummaryRedisKeyPrefix string
	SummaryTTL            time.Duration
	SummaryMaxDocChunks   int
	SummaryMaxTokens      int

	// Intelligence enrichment (feeder v2).
	IntelligenceEnabled bool
	SymbolLookupLimit   int
	FunctionSearchLimit int
	CallGraphDepth      int
	CallGraphMaxEdges   int
	RepoMapLimit        int
	MaxContextTokens    int
}

// Load reads configuration from the environment, applies defaults, and validates.
func Load() (*Config, error) {
	c := &Config{
		Port:                 envInt("PORT", 8081),
		OllamaURL:            envStr("OLLAMA_URL", "http://localhost:11434"),
		OllamaModel:          envStr("OLLAMA_MODEL", "qwen3:30b"),
		OllamaTimeout:        envDuration("OLLAMA_TIMEOUT", 180*time.Second),
		EmbedModel:           envStr("EMBED_MODEL", "qwen3-embedding"),
		EmbedTimeout:         envDuration("EMBED_TIMEOUT", 30*time.Second),
		WeaviateURL:          envStr("WEAVIATE_URL", "http://localhost:8080"),
		WeaviateClass:        envStr("WEAVIATE_CLASS", "RepoChunk"),
		WeaviateTimeout:      envDuration("WEAVIATE_TIMEOUT", 15*time.Second),
		DefaultTopK:          envInt("DEFAULT_TOP_K", 8),
		HybridAlpha:          envFloat("HYBRID_ALPHA", 0.65),
		ResolveRepoMinScore:  envFloat("RESOLVE_REPO_MIN_SCORE", 1.0),
		ResolveRepoAmbiguity: envFloat("RESOLVE_REPO_AMBIGUITY", 0.90),
		AnchorLen:            envInt("ANCHOR_LEN", 6),
		AnchorMin:            envInt("ANCHOR_MIN", 2),
		HTTPReadTimeout:      envDuration("HTTP_READ_TIMEOUT", 15*time.Second),
		HTTPWriteTimeout:     envDuration("HTTP_WRITE_TIMEOUT", 90*time.Second),
		HTTPIdleTimeout:      envDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		MaxBodyBytes:         int64(envInt("MAX_BODY_BYTES", 1048576)),
		APIKey:               envStr("API_KEY", ""),
		RateLimitRPS:         envFloat("RATE_LIMIT_RPS", 5),
		RateLimitBurst:       envInt("RATE_LIMIT_BURST", 10),
		LogLevel:             envStr("LOG_LEVEL", "info"),
		LogFormat:            envStr("LOG_FORMAT", "json"),

		RedisURL: envStr("REDIS_URL", "redis://localhost:6379/0"),

		DocsPathPrefix:        envStr("DOCS_PATH_PREFIX", "site/"),
		SummaryEnabled:        envBool("SUMMARY_ENABLED", true),
		SummaryRedisKeyPrefix: envStr("SUMMARY_REDIS_KEY_PREFIX", "analyzer:summary:"),
		SummaryTTL:            envDuration("SUMMARY_TTL", 24*time.Hour),
		SummaryMaxDocChunks:   envInt("SUMMARY_MAX_DOC_CHUNKS", 30),
		SummaryMaxTokens:      envInt("SUMMARY_MAX_TOKENS", 4096),

		IntelligenceEnabled: envBool("INTELLIGENCE_ENABLED", true),
		SymbolLookupLimit:   envInt("SYMBOL_LOOKUP_LIMIT", 10),
		FunctionSearchLimit: envInt("FUNCTION_SEARCH_LIMIT", 10),
		CallGraphDepth:      envInt("CALLGRAPH_DEPTH", 2),
		CallGraphMaxEdges:   envInt("CALLGRAPH_MAX_EDGES", 20),
		RepoMapLimit:        envInt("REPOMAP_LIMIT", 20),
		MaxContextTokens:    envInt("MAX_CONTEXT_TOKENS", 50000),
	}
	return c, c.validate()
}

func (c *Config) validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("PORT must be in [1,65535], got %d", c.Port)
	}
	if c.DefaultTopK < 1 {
		return fmt.Errorf("DEFAULT_TOP_K must be >= 1, got %d", c.DefaultTopK)
	}
	if c.HybridAlpha < 0 || c.HybridAlpha > 1 {
		return fmt.Errorf("HYBRID_ALPHA must be in [0,1], got %f", c.HybridAlpha)
	}
	if c.AnchorMin < 1 {
		return fmt.Errorf("ANCHOR_MIN must be >= 1, got %d", c.AnchorMin)
	}
	if c.AnchorLen < c.AnchorMin {
		return fmt.Errorf("ANCHOR_LEN (%d) must be >= ANCHOR_MIN (%d)", c.AnchorLen, c.AnchorMin)
	}
	for _, pair := range []struct{ name, val string }{
		{"OLLAMA_URL", c.OllamaURL},
		{"WEAVIATE_URL", c.WeaviateURL},
	} {
		if _, err := url.ParseRequestURI(pair.val); err != nil {
			return fmt.Errorf("%s: invalid URL %q: %w", pair.name, pair.val, err)
		}
	}
	if c.OllamaTimeout <= 0 {
		return fmt.Errorf("OLLAMA_TIMEOUT must be positive")
	}
	if c.WeaviateTimeout <= 0 {
		return fmt.Errorf("WEAVIATE_TIMEOUT must be positive")
	}
	if c.MaxBodyBytes < 1 {
		return fmt.Errorf("MAX_BODY_BYTES must be >= 1")
	}
	return nil
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
