// Package analyzer orchestrates the full analysis pipeline:
// logparse → retrieve → prompt → generate → parse → ground → calibrate.
package analyzer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/infoblox/vibecoder-analyzer/internal/calibration"
	"github.com/infoblox/vibecoder-analyzer/internal/config"
	"github.com/infoblox/vibecoder-analyzer/internal/grounding"
	"github.com/infoblox/vibecoder-analyzer/internal/issue"
	"github.com/infoblox/vibecoder-analyzer/internal/logparse"
	"github.com/infoblox/vibecoder-analyzer/internal/ollama"
	"github.com/infoblox/vibecoder-analyzer/internal/prompt"
	"github.com/infoblox/vibecoder-analyzer/internal/retriever"
	"github.com/infoblox/vibecoder-analyzer/internal/summarizer"
	"github.com/infoblox/vibecoder-analyzer/internal/weaviate"
)

// Query is the input to the analysis pipeline.
type Query struct {
	Input string // Raw error log or free-text prompt.
	Mode  string // "logs" or "prompt" (passed to logparse.Extract).
	Repo  string // Optional scoping hint.
	TopK  int    // Override for chunk limit (0 = use config default).
}

// Analyzer orchestrates the analysis pipeline.
type Analyzer struct {
	Retriever  *retriever.Retriever
	Generator  ollama.Generator
	Summarizer summarizer.Summarizer // optional; nil disables doc priming
	Cfg        *config.Config
}

// New creates an Analyzer. summ may be nil (no documentation priming).
func New(r *retriever.Retriever, g ollama.Generator, summ summarizer.Summarizer, cfg *config.Config) *Analyzer {
	return &Analyzer{Retriever: r, Generator: g, Summarizer: summ, Cfg: cfg}
}

// Run executes the full pipeline and returns a grounded, calibrated Issue.
func (a *Analyzer) Run(ctx context.Context, q Query) (*issue.Issue, error) {
	mode := q.Mode
	if mode == "" {
		mode = "logs"
	}
	topK := q.TopK
	if topK <= 0 {
		topK = a.Cfg.DefaultTopK
	}

	slog.Info("pipeline: start",
		"mode", mode,
		"top_k", topK,
		"repo_hint", q.Repo,
		"input_len", len(q.Input))

	// 1. Extract signals from input.
	slog.Info("pipeline: step 1/10 parsing input signals")
	signals := logparse.Extract(q.Input, mode)
	slog.Info("pipeline: step 1/10 done",
		"file_anchors", len(signals.FileFrames),
		"symbols", len(signals.Symbols),
		"error_type", signals.ErrorType)

	// 2. Retrieve relevant chunks from Weaviate.
	queryStr := signals.QueryString()
	if queryStr == "" {
		queryStr = q.Input
	}
	slog.Info("pipeline: step 2/10 retrieving chunks from weaviate",
		"query_len", len(queryStr),
		"top_k", topK)
	res, err := a.Retriever.Retrieve(ctx, queryStr, q.Repo, topK)
	if err != nil {
		slog.Error("pipeline: step 2/10 retrieval failed", "error", err)
		return nil, fmt.Errorf("retrieval: %w", err)
	}
	slog.Info("pipeline: step 2/10 done",
		"resolved_repo", res.ResolvedRepo,
		"chunks", len(res.Chunks))

	// 3. Documentation priming (§7a) — fetch (or cache-hit) the per-repo
	// service summary. v2 uses FileSummary class; v1 falls back to
	// site/ docs + LLM. Best-effort: any failure yields empty summary.
	var serviceSummary string
	if a.Summarizer != nil {
		slog.Info("pipeline: step 3/10 fetching service summary",
			"repo", res.ResolvedRepo)
		serviceSummary = a.Summarizer.Get(ctx, res.ResolvedRepo)
		slog.Info("pipeline: step 3/10 done", "summary_len", len(serviceSummary))
	} else {
		slog.Info("pipeline: step 3/10 skipped (summarizer disabled)")
	}

	// 4-7. Intelligence enrichment (feeder v2) — symbols, functions,
	// call graph, repo map. Best-effort: errors logged, not fatal.
	slog.Info("pipeline: steps 4-7/10 intelligence enrichment",
		"repo", res.ResolvedRepo,
		"symbols", len(signals.Symbols))
	intel := a.Retriever.Enrich(ctx, signals.Symbols, res.ResolvedRepo)
	slog.Info("pipeline: steps 4-7/10 done",
		"symbols_resolved", len(intel.Symbols),
		"functions", len(intel.Functions),
		"call_edges", len(intel.CallEdges),
		"repo_map_nodes", len(intel.RepoMap))

	// 8. Build LLM prompt with intelligence context.
	slog.Info("pipeline: step 8/10 building LLM prompt")
	system, user := prompt.Build(signals, res.Chunks, serviceSummary, intel)
	slog.Info("pipeline: step 8/10 done",
		"system_len", len(system),
		"user_len", len(user))

	// 9. Generate analysis via LLM.
	slog.Info("pipeline: step 9/10 calling LLM",
		"model", a.Cfg.OllamaModel,
		"timeout", a.Cfg.OllamaTimeout)
	raw, err := a.Generator.Generate(ctx, a.Cfg.OllamaModel, system, user)
	if err != nil {
		slog.Error("pipeline: step 9/10 generation failed", "error", err)
		return nil, fmt.Errorf("generation: %w", err)
	}
	slog.Info("pipeline: step 9/10 done", "raw_len", len(raw))

	// 10. Parse, ground, calibrate.
	slog.Info("pipeline: step 10/10 parse + ground + calibrate")
	iss, err := parseIssue(raw)
	if err != nil {
		slog.Error("pipeline: step 10/10 parse failed", "error", err)
		return nil, fmt.Errorf("parse LLM output: %w", err)
	}

	// Set resolved repo from retrieval.
	iss.ResolvedRepo = res.ResolvedRepo

	// Build references from retrieved chunks.
	iss.References = buildReferences(res.Chunks)

	// Ground — verify model claims against actual chunks.
	grounding.Verify(iss, res.Chunks, a.Cfg.AnchorLen, a.Cfg.AnchorMin)

	// Calibrate — deterministic confidence linting.
	calibration.Lint(iss)

	slog.Info("pipeline: step 10/10 done",
		"grounding_ok", iss.GroundingOK,
		"confidence", iss.Confidence)

	slog.Info("pipeline: complete",
		"resolved_repo", iss.ResolvedRepo,
		"grounding_ok", iss.GroundingOK,
		"confidence", iss.Confidence,
		"intelligence_symbols", len(intel.Symbols),
		"intelligence_functions", len(intel.Functions),
		"intelligence_edges", len(intel.CallEdges))

	return iss, nil
}

// thinkBlockRE matches reasoning blocks emitted by qwen3 / deepseek-r1 and
// similar models even when format=json is set.
var thinkBlockRE = regexp.MustCompile(`(?s)<think>.*?</think>`)

// parseIssue unmarshals the LLM JSON output into an Issue. It tolerates
// reasoning-model preambles (<think>…</think>), markdown code fences,
// and leading/trailing prose by extracting the first balanced JSON object.
func parseIssue(raw string) (*issue.Issue, error) {
	clean := extractJSONObject(raw)
	var iss issue.Issue
	if err := json.Unmarshal([]byte(clean), &iss); err != nil {
		preview := raw
		if len(preview) > 240 {
			preview = preview[:240] + "…"
		}
		return nil, fmt.Errorf("invalid JSON from LLM: %w (raw=%q)", err, preview)
	}
	return &iss, nil
}

// extractJSONObject returns the first top-level {…} JSON object found in s,
// after stripping <think> blocks and ```json fences. Falls back to the
// original string if no object is found (so the JSON error surfaces verbatim).
func extractJSONObject(s string) string {
	s = thinkBlockRE.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	// Strip ```json … ``` or ``` … ``` fences if present.
	if strings.HasPrefix(s, "```") {
		if i := strings.Index(s, "\n"); i >= 0 {
			s = s[i+1:]
		}
		if j := strings.LastIndex(s, "```"); j >= 0 {
			s = s[:j]
		}
		s = strings.TrimSpace(s)
	}
	// Walk to find the first balanced object, ignoring braces inside strings.
	start := strings.IndexByte(s, '{')
	if start < 0 {
		return s
	}
	depth := 0
	inStr := false
	esc := false
	for i := start; i < len(s); i++ {
		ch := s[i]
		if inStr {
			if esc {
				esc = false
				continue
			}
			if ch == '\\' {
				esc = true
				continue
			}
			if ch == '"' {
				inStr = false
			}
			continue
		}
		switch ch {
		case '"':
			inStr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return s
}

// buildReferences converts weaviate chunks to issue references.
func buildReferences(chunks []weaviate.Chunk) []issue.Reference {
	refs := make([]issue.Reference, len(chunks))
	for i, ch := range chunks {
		refs[i] = issue.Reference{
			Repo:      ch.Repo,
			FilePath:  ch.FilePath,
			StartLine: ch.StartLine,
			EndLine:   ch.EndLine,
			Content:   ch.Content,
			Language:  ch.Language,
			Score:     ch.Score,
		}
	}
	return refs
}
