package retriever

import (
	"context"
	"errors"
	"testing"

	"github.com/infoblox/vibecoder-analyzer/internal/config"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

// fakeSearcher returns canned results for testing.
type fakeSearcher struct {
	calls   []searchCall
	results map[string][]weaviate.Chunk // key: repo ("" for unscoped)
}

type searchCall struct {
	Query string
	Repo  string
	Limit int
}

func (f *fakeSearcher) Search(_ context.Context, query string, _ []float32, repo string, limit int, _ float64) ([]weaviate.Chunk, error) {
	f.calls = append(f.calls, searchCall{query, repo, limit})
	if chunks, ok := f.results[repo]; ok {
		if len(chunks) > limit {
			return chunks[:limit], nil
		}
		return chunks, nil
	}
	return nil, nil
}

func (f *fakeSearcher) FetchDocs(_ context.Context, _, _ string, _ int) ([]weaviate.Chunk, error) {
	return nil, nil
}

func TestRetrieve_RepoProvided(t *testing.T) {
	fs := &fakeSearcher{
		results: map[string][]weaviate.Chunk{
			"svc-a": {
				{Repo: "svc-a", FilePath: "main.go", Score: 0.9},
				{Repo: "svc-a", FilePath: "helper.go", Score: 0.7},
			},
		},
	}
	cfg := &config.Config{DefaultTopK: 8, HybridAlpha: 0.5}
	r := New(fs, nil, cfg)

	res, err := r.Retrieve(context.Background(), "panic", "svc-a", 8)
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if res.ResolvedRepo != "svc-a" {
		t.Errorf("ResolvedRepo = %q, want svc-a", res.ResolvedRepo)
	}
	if len(res.Chunks) != 2 {
		t.Errorf("got %d chunks, want 2", len(res.Chunks))
	}
	// Should have made exactly one search call (scoped).
	if len(fs.calls) != 1 {
		t.Errorf("expected 1 search call, got %d", len(fs.calls))
	}
	if fs.calls[0].Repo != "svc-a" {
		t.Errorf("search repo = %q, want svc-a", fs.calls[0].Repo)
	}
}

func TestRetrieve_RepoOmitted_ClearWinner(t *testing.T) {
	fs := &fakeSearcher{
		results: map[string][]weaviate.Chunk{
			"": { // broad search
				{Repo: "svc-a", Score: 0.9},
				{Repo: "svc-a", Score: 0.8},
				{Repo: "svc-a", Score: 0.7},
				{Repo: "svc-b", Score: 0.2},
			},
			"svc-a": { // scoped search
				{Repo: "svc-a", FilePath: "main.go", Score: 0.9},
				{Repo: "svc-a", FilePath: "helper.go", Score: 0.8},
			},
		},
	}
	cfg := &config.Config{
		DefaultTopK:          8,
		HybridAlpha:          0.5,
		ResolveRepoMinScore:  1.0,
		ResolveRepoAmbiguity: 0.90,
	}
	r := New(fs, nil, cfg)

	res, err := r.Retrieve(context.Background(), "panic", "", 8)
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if res.ResolvedRepo != "svc-a" {
		t.Errorf("ResolvedRepo = %q, want svc-a", res.ResolvedRepo)
	}
	// Two search calls: broad + scoped.
	if len(fs.calls) != 2 {
		t.Fatalf("expected 2 search calls, got %d", len(fs.calls))
	}
	if fs.calls[0].Repo != "" {
		t.Error("first call should be unscoped (broad)")
	}
	if fs.calls[1].Repo != "svc-a" {
		t.Errorf("second call repo = %q, want svc-a", fs.calls[1].Repo)
	}
}

func TestRetrieve_RepoOmitted_Ambiguous(t *testing.T) {
	fs := &fakeSearcher{
		results: map[string][]weaviate.Chunk{
			"": {
				{Repo: "svc-a", Score: 0.9},
				{Repo: "svc-b", Score: 0.88}, // s2/s1 = 0.977 > 0.90 → ambiguous
			},
		},
	}
	cfg := &config.Config{
		DefaultTopK:          8,
		HybridAlpha:          0.5,
		ResolveRepoMinScore:  1.0,
		ResolveRepoAmbiguity: 0.90,
	}
	r := New(fs, nil, cfg)

	_, err := r.Retrieve(context.Background(), "panic", "", 8)
	if err == nil {
		t.Fatal("expected ErrAmbiguousRepo, got nil")
	}
	var ambErr *ErrAmbiguousRepo
	if !errors.As(err, &ambErr) {
		t.Fatalf("expected ErrAmbiguousRepo, got %T: %v", err, err)
	}
	if len(ambErr.Candidates) != 2 {
		t.Errorf("expected 2 candidates, got %d", len(ambErr.Candidates))
	}
}

func TestRetrieve_RepoOmitted_BelowFloor(t *testing.T) {
	fs := &fakeSearcher{
		results: map[string][]weaviate.Chunk{
			"": {
				{Repo: "svc-a", Score: 0.3}, // sum = 0.3 < 1.0 floor
			},
		},
	}
	cfg := &config.Config{
		DefaultTopK:          8,
		HybridAlpha:          0.5,
		ResolveRepoMinScore:  1.0,
		ResolveRepoAmbiguity: 0.90,
	}
	r := New(fs, nil, cfg)

	_, err := r.Retrieve(context.Background(), "panic", "", 8)
	var ambErr *ErrAmbiguousRepo
	if !errors.As(err, &ambErr) {
		t.Fatalf("expected ErrAmbiguousRepo, got %T: %v", err, err)
	}
}
