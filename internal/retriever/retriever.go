// Package retriever performs hybrid search against Weaviate with optional
// multi-repo resolution (FR-A5 / FR-A5b) and intelligence enrichment (v2).
package retriever

import (
	"context"
	"fmt"
	"log/slog"
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

// Intelligence holds enrichment data from repo-indexer v2 classes.
type Intelligence struct {
	Symbols  []weaviate.Symbol
	Functions []weaviate.Function
	CallEdges []weaviate.CallEdge
	RepoMap   []weaviate.RepoMapNode
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
	Searcher     weaviate.Searcher
	Intelligence weaviate.IntelligenceSearcher // optional; nil disables enrichment
	Embedder     ollama.Embedder
	Cfg          *config.Config
}

// New creates a Retriever. embedder is required because the RepoChunk class
// uses vectorizer:none; pass nil only in tests that stub the Searcher and
// never exercise the embedding path. intel may be nil to disable enrichment.
func New(searcher weaviate.Searcher, intel weaviate.IntelligenceSearcher, embedder ollama.Embedder, cfg *config.Config) *Retriever {
	return &Retriever{Searcher: searcher, Intelligence: intel, Embedder: embedder, Cfg: cfg}
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

// Enrich performs intelligence enrichment against repo-indexer v2 classes.
// Returns zero-value Intelligence if the intelligence searcher is nil or
// intelligence is disabled. Errors are logged and swallowed (best-effort).
func (r *Retriever) Enrich(ctx context.Context, symbols []string, repo string) Intelligence {
	var intel Intelligence
	if r.Intelligence == nil || r.Cfg == nil || !r.Cfg.IntelligenceEnabled || repo == "" {
		return intel
	}

	// 1. Symbol lookup.
	if len(symbols) > 0 {
		syms, err := r.Intelligence.LookupSymbols(ctx, repo, symbols, r.Cfg.SymbolLookupLimit)
		if err != nil {
			slog.Warn("intelligence: symbol lookup failed", "error", err)
		} else {
			intel.Symbols = syms
		}
	}

	// 2. Function search — extract function/method names from resolved symbols.
	funcNames := extractFuncNames(intel.Symbols, symbols)
	if len(funcNames) > 0 {
		fns, err := r.Intelligence.SearchFunctions(ctx, repo, funcNames, r.Cfg.FunctionSearchLimit)
		if err != nil {
			slog.Warn("intelligence: function search failed", "error", err)
		} else {
			intel.Functions = fns
		}
	}

	// 3. Call graph traversal from resolved function names.
	if len(funcNames) > 0 {
		edges, err := r.Intelligence.GetCallGraph(ctx, repo, funcNames, r.Cfg.CallGraphDepth, r.Cfg.CallGraphMaxEdges)
		if err != nil {
			slog.Warn("intelligence: call graph failed", "error", err)
		} else {
			intel.CallEdges = edges
		}
	}

	// 4. Repository map.
	nodes, err := r.Intelligence.GetRepoMap(ctx, repo, r.Cfg.RepoMapLimit)
	if err != nil {
		slog.Warn("intelligence: repo map failed", "error", err)
	} else {
		intel.RepoMap = nodes
	}

	return intel
}

// extractFuncNames collects function/method names from resolved symbols
// plus any raw symbol that looks like a function call (contains a dot).
func extractFuncNames(resolved []weaviate.Symbol, rawSymbols []string) []string {
	seen := map[string]bool{}
	var names []string

	for _, sym := range resolved {
		if sym.Type == "function" || sym.Type == "method" {
			name := sym.Name
			if !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
	}

	// Also try raw symbols that might be qualified function names.
	for _, s := range rawSymbols {
		if !seen[s] {
			seen[s] = true
			names = append(names, s)
		}
	}

	return names
}
