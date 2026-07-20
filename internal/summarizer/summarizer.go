// Package summarizer produces and caches per-repo service summaries.
// v2: backed by the repo-indexer's FileSummary Weaviate class — no LLM
// summarization call needed. Summaries are stored in Redis under
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
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

// Summarizer returns a short prose description of a repo, suitable
// for prefixing the analyzer's LLM prompt.
type Summarizer interface {
	Get(ctx context.Context, repo string) string
}

// fileSummaryFetcher is the subset of weaviate.IntelligenceSearcher the summarizer needs.
type fileSummaryFetcher interface {
	GetFileSummaries(ctx context.Context, repo string, limit int) ([]weaviate.FileSummary, error)
}

// docFetcher is the subset of weaviate.Searcher the summarizer needs (v1 fallback).
type docFetcher interface {
	FetchDocs(ctx context.Context, repo, pathPrefix string, limit int) ([]weaviate.Chunk, error)
}

// Cache implements Summarizer with a Redis cache. If RedisClient is nil
// (Redis unavailable, opt-out), Get computes the summary every call
// without caching. If the summarizer is disabled at the config level
// it returns "" immediately and Get is a no-op.
type Cache struct {
	Cfg              *config.Config
	Redis            *redis.Client // optional; nil disables caching
	FileSummaryFetch fileSummaryFetcher // v2: fetches FileSummary objects
	DocFetch         docFetcher         // v1 fallback: fetches site/ docs
	Generator        generator          // v1 fallback: LLM summarization
}

// generator abstracts LLM generation (v1 fallback only).
type generator interface {
	Generate(ctx context.Context, model, system, prompt string) (string, error)
}

// New constructs a Cache. rdb may be nil. fsf may be nil (v1 mode).
// When fsf is provided, the summarizer uses FileSummary objects directly.
// When fsf is nil, falls back to FetchDocs + LLM summarization.
func New(cfg *config.Config, rdb *redis.Client, fsf fileSummaryFetcher, docFetch docFetcher, gen generator) *Cache {
	return &Cache{
		Cfg:              cfg,
		Redis:            rdb,
		FileSummaryFetch: fsf,
		DocFetch:         docFetch,
		Generator:        gen,
	}
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

// build fetches file summaries and concatenates them. Uses FileSummary
// class (v2) when available, falls back to FetchDocs + LLM (v1).
func (c *Cache) build(ctx context.Context, repo string) (string, error) {
	// v2 path: use pre-built FileSummary objects from the repo-indexer.
	if c.FileSummaryFetch != nil {
		return c.buildFromFileSummaries(ctx, repo)
	}

	// v1 fallback: fetch site/ docs and summarize via LLM.
	return c.buildFromDocs(ctx, repo)
}

// buildFromFileSummaries concatenates repo-indexer-generated file summaries.
func (c *Cache) buildFromFileSummaries(ctx context.Context, repo string) (string, error) {
	summaries, err := c.FileSummaryFetch.GetFileSummaries(ctx, repo, c.Cfg.SummaryMaxDocChunks)
	if err != nil {
		return "", fmt.Errorf("fetch file summaries: %w", err)
	}
	if len(summaries) == 0 {
		slog.Info("no FileSummary objects found; skipping summary",
			"repo", repo)
		return "", nil
	}

	var b strings.Builder
	for _, fs := range summaries {
		b.WriteString(fs.FilePath)
		if fs.Package != "" {
			b.WriteString(" [")
			b.WriteString(fs.Package)
			b.WriteString("]")
		}
		b.WriteString(": ")
		b.WriteString(strings.TrimSpace(fs.Summary))
		b.WriteString("\n")
		// Structured is repo-indexer's per-file breakdown from the same
		// LLM call that produced Summary above (nil/empty for repos
		// indexed before that feature shipped — both are then no-ops).
		// Of the fields available, external_calls and notes carry the
		// highest signal for the analyzer's job (tracing where a panic
		// crossed a package boundary, or a documented gotcha the LLM
		// flagged) without bloating the prompt with structural info
		// (structs/interfaces/imports) that mostly restates the code.
		if len(fs.ExternalCalls) > 0 {
			b.WriteString("  calls: ")
			b.WriteString(strings.Join(fs.ExternalCalls, ", "))
			b.WriteString("\n")
		}
		if len(fs.Notes) > 0 {
			b.WriteString("  notes: ")
			b.WriteString(strings.Join(fs.Notes, "; "))
			b.WriteString("\n")
		}
	}
	return b.String(), nil
}

// buildFromDocs is the v1 fallback: fetch site/ docs and summarize via LLM.
func (c *Cache) buildFromDocs(ctx context.Context, repo string) (string, error) {
	if c.DocFetch == nil || c.Generator == nil {
		return "", nil
	}

	chunks, err := c.DocFetch.FetchDocs(ctx, repo, c.Cfg.DocsPathPrefix, c.Cfg.SummaryMaxDocChunks)
	if err != nil {
		return "", fmt.Errorf("fetch docs: %w", err)
	}
	if len(chunks) == 0 {
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
