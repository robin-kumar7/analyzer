package summarizer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/infoblox/vibecoder-analyzer/internal/config"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

// fakeDocFetcher implements docFetcher.
type fakeDocFetcher struct {
	calls  int32
	chunks []weaviate.Chunk
	err    error
}

func (f *fakeDocFetcher) FetchDocs(_ context.Context, _, _ string, _ int) ([]weaviate.Chunk, error) {
	atomic.AddInt32(&f.calls, 1)
	return f.chunks, f.err
}

// fakeGenerator implements ollama.Generator.
type fakeGenerator struct {
	calls int32
	out   string
	err   error
}

func (g *fakeGenerator) Generate(_ context.Context, _, _, _ string) (string, error) {
	atomic.AddInt32(&g.calls, 1)
	return g.out, g.err
}

func baseCfg() *config.Config {
	return &config.Config{
		SummaryEnabled:        true,
		SummaryRedisKeyPrefix: "analyzer:summary:",
		DocsPathPrefix:        "site/",
		SummaryMaxDocChunks:   30,
		SummaryMaxTokens:      4096,
		OllamaModel:           "test",
	}
}

func TestGet_DisabledByConfig(t *testing.T) {
	cfg := baseCfg()
	cfg.SummaryEnabled = false
	fetcher := &fakeDocFetcher{chunks: []weaviate.Chunk{{Content: "doc"}}}
	gen := &fakeGenerator{out: "summary"}
	c := New(cfg, nil, nil, fetcher, gen)

	got := c.Get(context.Background(), "repo-a")
	if got != "" {
		t.Errorf("expected empty summary when disabled, got %q", got)
	}
	if fetcher.calls != 0 || gen.calls != 0 {
		t.Errorf("disabled summarizer should not call dependencies; fetcher=%d gen=%d",
			fetcher.calls, gen.calls)
	}
}

func TestGet_EmptyRepo(t *testing.T) {
	cfg := baseCfg()
	c := New(cfg, nil, nil, &fakeDocFetcher{}, &fakeGenerator{out: "x"})
	if got := c.Get(context.Background(), ""); got != "" {
		t.Errorf("expected empty summary for empty repo, got %q", got)
	}
}

func TestGet_NoDocsReturnsEmptyAndDoesNotCallOllama(t *testing.T) {
	cfg := baseCfg()
	fetcher := &fakeDocFetcher{chunks: nil}
	gen := &fakeGenerator{out: "should not be called"}
	c := New(cfg, nil, nil, fetcher, gen)

	got := c.Get(context.Background(), "repo-without-site")
	if got != "" {
		t.Errorf("expected empty summary when no docs, got %q", got)
	}
	if fetcher.calls != 1 {
		t.Errorf("fetcher calls = %d, want 1", fetcher.calls)
	}
	if gen.calls != 0 {
		t.Errorf("generator should not be called when no docs; calls = %d", gen.calls)
	}
}

func TestGet_BuildsAndReturnsSummary_NoCache(t *testing.T) {
	cfg := baseCfg()
	fetcher := &fakeDocFetcher{chunks: []weaviate.Chunk{
		{FilePath: "site/content/_index.md", Content: "This service handles payments."},
	}}
	gen := &fakeGenerator{out: "Payments service. Routes: POST /charge. Deps: postgres."}
	c := New(cfg, nil, nil, fetcher, gen)

	got := c.Get(context.Background(), "billing-svc")
	if got != gen.out {
		t.Errorf("got %q, want %q", got, gen.out)
	}
	if fetcher.calls != 1 || gen.calls != 1 {
		t.Errorf("expected 1 fetch + 1 generate; got fetch=%d gen=%d", fetcher.calls, gen.calls)
	}

	// Second call without Redis still recomputes (no cache).
	_ = c.Get(context.Background(), "billing-svc")
	if fetcher.calls != 2 || gen.calls != 2 {
		t.Errorf("without redis, summary should be recomputed; got fetch=%d gen=%d",
			fetcher.calls, gen.calls)
	}
}

func TestGet_FetchErrorReturnsEmpty(t *testing.T) {
	cfg := baseCfg()
	fetcher := &fakeDocFetcher{err: errors.New("weaviate down")}
	gen := &fakeGenerator{out: "x"}
	c := New(cfg, nil, nil, fetcher, gen)

	if got := c.Get(context.Background(), "repo"); got != "" {
		t.Errorf("expected empty summary on fetch error, got %q", got)
	}
	if gen.calls != 0 {
		t.Errorf("generator should not run when fetch fails; calls = %d", gen.calls)
	}
}

func TestGet_OllamaErrorReturnsEmpty(t *testing.T) {
	cfg := baseCfg()
	fetcher := &fakeDocFetcher{chunks: []weaviate.Chunk{{FilePath: "site/_index.md", Content: "doc"}}}
	gen := &fakeGenerator{err: errors.New("ollama timeout")}
	c := New(cfg, nil, nil, fetcher, gen)

	if got := c.Get(context.Background(), "repo"); got != "" {
		t.Errorf("expected empty summary on ollama error, got %q", got)
	}
}

func TestGet_TruncatesToMaxTokens(t *testing.T) {
	cfg := baseCfg()
	cfg.SummaryMaxTokens = 10
	fetcher := &fakeDocFetcher{chunks: []weaviate.Chunk{{FilePath: "site/_index.md", Content: "doc"}}}
	gen := &fakeGenerator{out: "this summary is definitely longer than ten chars"}
	c := New(cfg, nil, nil, fetcher, gen)

	got := c.Get(context.Background(), "repo")
	if len(got) != 10 {
		t.Errorf("expected truncated len 10, got %d (%q)", len(got), got)
	}
}

func TestGet_NilReceiverSafe(t *testing.T) {
	var c *Cache
	if got := c.Get(context.Background(), "repo"); got != "" {
		t.Errorf("nil cache should return empty string, got %q", got)
	}
}
