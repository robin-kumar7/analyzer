// Package retriever performs hybrid search against Weaviate with optional
// multi-repo resolution (FR-A5 / FR-A5b).
package retriever

import (
	"context"
	"fmt"
	"sort"

	"github.com/infoblox/vibecoder-analyzer/internal/config"
	"github.com/infoblox/vibecoder-analyzer/internal/ollama"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

// Result holds retrieval output.
type Result struct {
	Chunks       []weaviate.Chunk
	ResolvedRepo string
}

// ErrAmbiguousRepo is returned when the repo cannot be determined
// unambiguously from the retrieval scores (FR-A5b).
type ErrAmbiguousRepo struct {
	Candidates []string
}

func (e *ErrAmbiguousRepo) Error() string {
	return fmt.Sprintf("ambiguous repo: candidates are %v", e.Candidates)
}

// Retriever searches Weaviate and resolves the target repo.
type Retriever struct {
	Searcher weaviate.Searcher
	Embedder ollama.Embedder
	Cfg      *config.Config
}

// New creates a Retriever. embedder is required because the RepoChunk class
// uses vectorizer:none; pass nil only in tests that stub the Searcher and
// never exercise the embedding path.
func New(searcher weaviate.Searcher, embedder ollama.Embedder, cfg *config.Config) *Retriever {
	return &Retriever{Searcher: searcher, Embedder: embedder, Cfg: cfg}
}

// Retrieve performs hybrid search. When repo is provided, results are scoped
// to that repo via a where clause (FR-A5). When repo is empty, two-pass
// resolution is used (FR-A5b).
func (r *Retriever) Retrieve(ctx context.Context, query, repo string, topK int) (Result, error) {
	if topK <= 0 {
		topK = r.Cfg.DefaultTopK
	}

	vector, err := r.embedQuery(ctx, query)
	if err != nil {
		return Result{}, fmt.Errorf("embed query: %w", err)
	}

	if repo != "" {
		chunks, err := r.Searcher.Search(ctx, query, vector, repo, topK, r.Cfg.HybridAlpha)
		if err != nil {
			return Result{}, fmt.Errorf("scoped search: %w", err)
		}
		return Result{Chunks: chunks, ResolvedRepo: repo}, nil
	}

	return r.resolveRepo(ctx, query, vector, topK)
}

// embedQuery returns the dense vector for the query, or nil when no embedder
// is configured (tests). A nil vector forces a BM25-only search.
func (r *Retriever) embedQuery(ctx context.Context, query string) ([]float32, error) {
	if r.Embedder == nil || r.Cfg == nil || r.Cfg.EmbedModel == "" {
		return nil, nil
	}
	return r.Embedder.Embed(ctx, r.Cfg.EmbedModel, query)
}

// resolveRepo implements FR-A5b: deterministic, pre-model repo selection.
func (r *Retriever) resolveRepo(ctx context.Context, query string, vector []float32, topK int) (Result, error) {
	// Pass 1: broad search across all repos.
	broadLimit := topK * 3
	if broadLimit < 15 {
		broadLimit = 15
	}
	allChunks, err := r.Searcher.Search(ctx, query, vector, "", broadLimit, r.Cfg.HybridAlpha)
	if err != nil {
		return Result{}, fmt.Errorf("broad search: %w", err)
	}
	if len(allChunks) == 0 {
		return Result{}, nil
	}

	// Aggregate scores by repo.
	repoScores := map[string]float64{}
	for _, ch := range allChunks {
		repoScores[ch.Repo] += ch.Score
	}

	type repoScore struct {
		Name  string
		Score float64
	}
	ranked := make([]repoScore, 0, len(repoScores))
	for name, score := range repoScores {
		ranked = append(ranked, repoScore{name, score})
	}
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].Score > ranked[j].Score
	})

	s1 := ranked[0].Score

	// Floor check.
	if s1 < r.Cfg.ResolveRepoMinScore {
		candidates := make([]string, len(ranked))
		for i, rs := range ranked {
			candidates[i] = rs.Name
		}
		return Result{}, &ErrAmbiguousRepo{Candidates: candidates}
	}

	// Ambiguity check.
	if len(ranked) > 1 {
		s2 := ranked[1].Score
		if s2 > 0 && s2/s1 > r.Cfg.ResolveRepoAmbiguity {
			candidates := make([]string, len(ranked))
			for i, rs := range ranked {
				candidates[i] = rs.Name
			}
			return Result{}, &ErrAmbiguousRepo{Candidates: candidates}
		}
	}

	// Pass 2: scoped search with the winner.
	winner := ranked[0].Name
	scopedChunks, err := r.Searcher.Search(ctx, query, vector, winner, topK, r.Cfg.HybridAlpha)
	if err != nil {
		return Result{}, fmt.Errorf("scoped search for %q: %w", winner, err)
	}

	return Result{Chunks: scopedChunks, ResolvedRepo: winner}, nil
}
