// Package summarizer produces and caches per-repo service summaries
// derived from each repo's `site/` Hugo documentation (see §7a of
// docs/04-analyzer-service.md). Summaries are stored in Redis under
// `<prefix><repo>` with a TTL; lookups are best-effort and never
// block the request.
package summarizer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/infoblox/vibecoder-analyzer/internal/config"
	"github.com/infoblox/vibecoder-analyzer/internal/ollama"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

// Summarizer returns a short prose description of a repo, suitable
// for prefixing the analyzer's LLM prompt.
type Summarizer interface {
	Get(ctx context.Context, repo string) string
}

// docFetcher is the subset of weaviate.Searcher the summarizer needs.
type docFetcher interface {
	FetchDocs(ctx context.Context, repo, pathPrefix string, limit int) ([]weaviate.Chunk, error)
}

// Cache implements Summarizer with a Redis cache. If RedisClient is nil
// (Redis unavailable, opt-out), Get computes the summary every call
// without caching. If the summarizer is disabled at the config level
// it returns "" immediately and Get is a no-op.
type Cache struct {
	Cfg       *config.Config
	Redis     *redis.Client // optional; nil disables caching
	Searcher  docFetcher
	Generator ollama.Generator
}

// New constructs a Cache. rdb may be nil.
func New(cfg *config.Config, rdb *redis.Client, s docFetcher, g ollama.Generator) *Cache {
	return &Cache{Cfg: cfg, Redis: rdb, Searcher: s, Generator: g}
}

// Get returns a service summary for repo. Always returns a string;
// errors are logged at warn and yield "".
func (c *Cache) Get(ctx context.Context, repo string) string {
	if c == nil || c.Cfg == nil || !c.Cfg.SummaryEnabled || repo == "" {
		return ""
	}

	key := c.Cfg.SummaryRedisKeyPrefix + repo

	// 1. Try cache.
	if c.Redis != nil {
		val, err := c.Redis.Get(ctx, key).Result()
		if err == nil && val != "" {
			return val
		}
		if err != nil && !errors.Is(err, redis.Nil) {
			slog.Warn("summary cache GET failed", "repo", repo, "error", err)
		}
	}

	// 2. Build summary.
	summary, err := c.build(ctx, repo)
	if err != nil {
		slog.Warn("summary build failed; proceeding without service context",
			"repo", repo, "error", err)
		return ""
	}
	if summary == "" {
		return ""
	}

	// 3. Cap length to bound prompt size.
	if c.Cfg.SummaryMaxTokens > 0 && len(summary) > c.Cfg.SummaryMaxTokens {
		summary = summary[:c.Cfg.SummaryMaxTokens]
	}

	// 4. Best-effort write-back.
	if c.Redis != nil {
		ttl := c.Cfg.SummaryTTL
		if ttl <= 0 {
			ttl = 24 * time.Hour
		}
		if err := c.Redis.Set(ctx, key, summary, ttl).Err(); err != nil {
			slog.Warn("summary cache SET failed", "repo", repo, "error", err)
		}
	}

	return summary
}

// build fetches doc chunks from Weaviate and asks Ollama to summarize them.
func (c *Cache) build(ctx context.Context, repo string) (string, error) {
	chunks, err := c.Searcher.FetchDocs(ctx, repo, c.Cfg.DocsPathPrefix, c.Cfg.SummaryMaxDocChunks)
	if err != nil {
		return "", fmt.Errorf("fetch docs: %w", err)
	}
	if len(chunks) == 0 {
		// No site/ folder for this repo; not an error, just nothing to summarize.
		slog.Info("no documentation chunks found; skipping summary",
			"repo", repo, "prefix", c.Cfg.DocsPathPrefix)
		return "", nil
	}

	system := summarySystemPrompt
	user := buildSummaryUser(repo, chunks)

	out, err := c.Generator.Generate(ctx, c.Cfg.OllamaModel, system, user)
	if err != nil {
		return "", fmt.Errorf("ollama summarize: %w", err)
	}
	return strings.TrimSpace(out), nil
}

const summarySystemPrompt = `You are a senior engineer reading the documentation of a software service.
Produce a SHORT, factual summary (10-25 lines) of what the service does, written for another engineer
who must root-cause production errors in it.

Cover, in this order:
1. One-sentence purpose of the service.
2. Main external interfaces (HTTP routes, queues, jobs).
3. Internal packages/modules and what each one is responsible for.
4. Key external dependencies (databases, message brokers, third-party APIs).
5. Notable invariants, error-handling conventions, or known failure modes mentioned in the docs.

Output PLAIN TEXT only — no markdown headers, no JSON, no preamble. Do not invent details
that are not present in the supplied documentation chunks.`

func buildSummaryUser(repo string, chunks []weaviate.Chunk) string {
	var b strings.Builder
	b.WriteString("Repository: ")
	b.WriteString(repo)
	b.WriteString("\n\nDocumentation chunks (from the repo's site/ folder):\n\n")
	for i, ch := range chunks {
		fmt.Fprintf(&b, "[%d] %s\n", i+1, ch.FilePath)
		b.WriteString(ch.Content)
		if !strings.HasSuffix(ch.Content, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	return b.String()
}
